import tkinter as tk
from tkinter import ttk, messagebox
from PIL import Image, ImageTk
import requests
import json
import io
from datetime import timezone, timedelta, datetime
from collections import defaultdict
import pyodbc
import win32print
import win32api
from PyPDF2 import PdfReader, PdfWriter
import tempfile
from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import letter
from pdf2image import convert_from_path
from PIL import ImageWin, Image
import win32ui
import subprocess
import os
import shutil
from concurrent.futures import ThreadPoolExecutor, as_completed
import zipfile
import fitz  # pip install pymupdf
import time
from typing import Optional
from datetime import datetime as dt

# === CONFIGURACIÓN ===
ML_CLIENT_ID = "450468213037845"
ML_CLIENT_SECRET = "lk49yNFU7hFabb7iaGfkbWcOH30IPtr4"
DROPBOX_LOG_PATH = os.path.join(os.path.expanduser("~"), "Dropbox", "Tickets Dragon", "DEPO")

# === DATOS GLOBALES ===
pedidos_filtrados = []
fila_widgets = []
fotos_cache = []
token = ""
seller_id = ""
ML_REFRESH_TOKEN = "TG-67acad136e3bf90001413727-209611492"
entry_codigo = None
resumen_label = None
impresora_seleccionada = "Xprinter XP-410B"  # Selección por defecto  
last_token_time = None

def obtener_access_token():
    global token, seller_id, last_token_time, ML_REFRESH_TOKEN
    now = time.time()
    if token and last_token_time and (now - last_token_time) < 3 * 3600:
        return token, seller_id
    
    print("Refrescando token de acceso...")
    url = "https://api.mercadolibre.com/oauth/token"
    data = {
        "grant_type": "refresh_token",
        "client_id": ML_CLIENT_ID,
        "client_secret": ML_CLIENT_SECRET,
        "refresh_token": ML_REFRESH_TOKEN
    }
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    resp = requests.post(url, data=data, headers=headers)
    if resp.status_code == 200:
        t = resp.json()
        token = t.get("access_token")
        seller_id = str(t.get("user_id"))
        new_refresh_token = t.get("refresh_token")
        if new_refresh_token:
            ML_REFRESH_TOKEN = new_refresh_token
            print("Nuevo refresh token guardado en memoria.")
        last_token_time = now
        return token, seller_id
    else:
        print(f"Error al refrescar el token: {resp.text}")
        raise Exception("No se pudo obtener el access token")

def listar_pedidos(seller_id, access_token, fecha_desde, fecha_hasta):
    offset, limit = 50, 50
    pedidos = []
    while True:
        if 'T' in fecha_desde:
            from_str = fecha_desde
        else:
            from_str = f"{fecha_desde}T00:00:00.000-00:00"
        if 'T' in fecha_hasta:
            to_str = fecha_hasta
        else:
            to_str = f"{fecha_hasta}T23:59:59.000-00:00"
        url = f"https://api.mercadolibre.com/orders/search?seller={seller_id}&offset={offset - limit}&limit={limit}&order.date_created.from={from_str}&order.date_created.to={to_str}"
        headers = {"Authorization": f"Bearer {access_token}"}
        resp = requests.get(url, headers=headers)
        if resp.status_code != 200:
            break
        data = resp.json()
        results = data.get("results", [])
        if not results:
            break
        pedidos.extend(results)
        if len(results) < limit:
            break
        offset += limit
    return pedidos

def obtener_nota(order_id, access_token):
    url = f"https://api.mercadolibre.com/orders/{order_id}/notes"
    r = requests.get(url, headers={"Authorization": f"Bearer {access_token}"})
    if r.status_code == 200:
        arr = r.json()
        if arr and arr[0].get("results"):
            return arr[0]["results"][0].get("note", "")
    return ""

def descargar_etiquetas_zpl(shipping_ids, access_token):
    if not shipping_ids:
        return None
    ids_str = ",".join(map(str, shipping_ids))
    url = (
        f"https://api.mercadolibre.com/shipment_labels"
        f"?shipment_ids={ids_str}"
        f"&response_type=zpl2"
    )
    headers = {"Authorization": f"Bearer {access_token}"}
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        zip_data = io.BytesIO(response.content)
        with zipfile.ZipFile(zip_data, 'r') as zip_ref:
            for file_name in zip_ref.namelist():
                if file_name.endswith('.txt') or file_name.endswith('.zpl'):
                    zpl_content = zip_ref.read(file_name)
                    return zpl_content
    return None


def nombre_completo(item):
    titulo = item.get("item", {}).get("title", "")
    variaciones = item.get("item", {}).get("variation_attributes", [])
    detalles = " ".join(f"{v.get('value_name','')}" for v in variaciones if v.get("value_name"))
    return f"{titulo} {detalles}".strip()

def nombre_pedido(pedido):
    items = pedido.get("order_items", [])
    if len(items) > 1:
        comprador = pedido.get("buyer", {}).get("nickname", "Comprador")
        return f"Varios Items ({len(items)}) - {comprador}"
    elif items:
        return nombre_completo(items[0])
    return "Pedido sin items"

def elegir_impresora():
    global impresora_seleccionada
    impresoras = [p[2] for p in win32print.EnumPrinters(win32print.PRINTER_ENUM_LOCAL | win32print.PRINTER_ENUM_CONNECTIONS)]
    win = tk.Toplevel()
    win.title("Seleccionar impresora")
    tk.Label(win, text="Elija una impresora:").pack(pady=5)
    lista = tk.Listbox(win, width=50)
    for imp in impresoras:
        lista.insert(tk.END, imp)
    lista.pack(padx=10, pady=10)
    def seleccionar():
        global impresora_seleccionada
        seleccion = lista.curselection()
        if seleccion:
            impresora_seleccionada = lista.get(seleccion[0])
            print(f"Impresora seleccionada: {impresora_seleccionada}")
            messagebox.showinfo("Impresora seleccionada", f"Impresora: {impresora_seleccionada}")
            win.destroy()
    tk.Button(win, text="Seleccionar", command=seleccionar).pack(pady=5)

def imprimir_zebra(zpl_data):
    import win32print
    print("Entrando a imprimir_zebra")
    
    print("ZPL recibido, tamaño:", len(zpl_data))
    try:
        zpl_text = zpl_data.decode("utf-8", errors="replace")
    except Exception:
        zpl_text = str(zpl_data)
    print("ZPL a imprimir:\n", zpl_text)
    hPrinter = win32print.OpenPrinter(impresora_seleccionada)
    print("Impresora abierta:", impresora_seleccionada)
    try:
        hJob = win32print.StartDocPrinter(hPrinter, 1, ("Zebra Job", "", "RAW"))
        print("Job iniciado")
        win32print.StartPagePrinter(hPrinter)
        print("Página iniciada")
        win32print.WritePrinter(hPrinter, zpl_data)
        print("Datos enviados")
        win32print.EndPagePrinter(hPrinter)
        print("Página terminada")
        win32print.EndDocPrinter(hPrinter)
        print("Job terminado")
    finally:
        win32print.ClosePrinter(hPrinter)
        print("Impresora cerrada")

def obtener_substatus_real_pedido(p, token):
    order_id = p.get("id")
    nota = obtener_nota(order_id, token)
    nota_upper = nota.upper()

    # Keywords a buscar (en mayúsculas)
    keywords = ['DEPO', 'MUNDOAL', 'MTGBBL', 'BBPS', 'MONBAHIA', 'MTGBBPS']

    # Si ninguna de las keywords está en la nota, se descarta el pedido
    if not any(keyword in nota_upper for keyword in keywords):
        return None

    shipping_id = p.get("shipping", {}).get("id")
    substatus_real = None
    if shipping_id:
        shipment_url = f"https://api.mercadolibre.com/shipments/{shipping_id}"
        shipment_resp = requests.get(shipment_url, headers={"Authorization": f"Bearer {token}"})
        if shipment_resp.status_code == 200:
            shipment_data = shipment_resp.json()
            substatus_real = shipment_data.get("substatus")
    
    if substatus_real and substatus_real.lower() in ["printed", "ready_to_print"]:
        p["nota"] = nota
        if "shipping" not in p:
            p["shipping"] = {}
        p["shipping"]["substatus_real"] = substatus_real
        return p
    return None

def mostrar_tabla_depo(desde, hasta):
    ventana = tk.Toplevel()
    ventana.title("DEPO")

    # Crear ventana separada para el resumen
    resumen_ventana = tk.Toplevel(ventana)
    resumen_ventana.title("Resumen de seleccionados")
    resumen_ventana.geometry("350x500")
    global resumen_label
    resumen_label = tk.Label(resumen_ventana, text="", justify="left", anchor="nw", font=("Arial", 14, "bold"), fg="blue", bg="#f0f0f0")
    resumen_label.pack(side="top", fill="both", expand=True, padx=10, pady=10)

    # --- IDs de pedidos ya mostrados (para detectar nuevos) ---
    if not hasattr(mostrar_tabla_depo, 'ids_anteriores'):
        mostrar_tabla_depo.ids_anteriores = set()
    ids_anteriores = set(mostrar_tabla_depo.ids_anteriores)
    primera_vez = len(ids_anteriores) == 0

    # --- Botón para elegir impresora ---
    btn_impresora = tk.Button(ventana, text="Elegir impresora", command=elegir_impresora)
    btn_impresora.pack(pady=5)

    # --- Botón para actualizar y nuevos checkboxes ---
    top_controls = tk.Frame(ventana)
    top_controls.pack(pady=5)
    btn_actualizar = tk.Button(top_controls, text="Actualizar", bg="#6ec6ff")
    btn_actualizar.pack(side="left", padx=2)
    incluir_impresos_var = tk.BooleanVar(value=False)
    chk_incluir_impresos = tk.Checkbutton(top_controls, text="Incluir ya impresos", variable=incluir_impresos_var)
    chk_incluir_impresos.pack(side="left", padx=2)
    hasta_13_var = tk.BooleanVar(value=False)
    chk_hasta_13 = tk.Checkbutton(top_controls, text="Hasta las 13:00", variable=hasta_13_var)
    chk_hasta_13.pack(side="left", padx=2)

    def actualizar_tabla():
        global pedidos_filtrados, token, seller_id
        try:
            dt1 = datetime.strptime(desde, "%d/%m/%Y")
            dt2 = datetime.strptime(hasta, "%d/%m/%Y")
        except:
            messagebox.showerror("Formato incorrecto", "Usar formato DD/MM/YYYY")
            return
        desde_api = dt1.strftime("%Y-%m-%d")
        # --- hasta_13: si está tildado, la fecha hasta es hasta las 13:00 ---
        if hasta_13_var.get():
            hasta_api = dt2.strftime("%Y-%m-%dT13:00:00.000-03:00")
        else:
            hasta_api = dt2.strftime("%Y-%m-%dT23:59:59.000-00:00")
        token, seller_id = obtener_access_token()
        # Modificar listar_pedidos para aceptar el string completo de hasta_api
        pedidos = listar_pedidos(seller_id, token, desde_api, hasta_api)
        nuevos_filtrados = []
        with ThreadPoolExecutor(max_workers=8) as executor:
            futuros = [executor.submit(obtener_substatus_real_pedido, p, token) for p in pedidos]
            for futuro in as_completed(futuros):
                res = futuro.result()
                if res:
                    nuevos_filtrados.append(res)
        nuevos_ids = {p['id'] for p in nuevos_filtrados}
        mostrar_tabla_depo.ids_anteriores = nuevos_ids
        # Limpiar widgets previos
        for w in fila_widgets:
            w.destroy()
        fila_widgets.clear()
        pedidos_filtrados.clear()
        # --- Filtrado según checkbox ---
        if incluir_impresos_var.get():
            pedidos_filtrados.extend(nuevos_filtrados)
        else:
            pedidos_filtrados.extend([p for p in nuevos_filtrados if p.get('shipping', {}).get('substatus_real', '').lower() != 'printed'])
        # Detectar si es la primera vez (ids_anteriores vacío)
        primera_vez_local = len(ids_anteriores) == 0
        for i, pedido in enumerate(pedidos_filtrados):
            pedido_id = pedido.get("pack_id") or pedido.get("id", "SIN ID")
            nota = pedido.get("nota", "")
            order_items = pedido.get("order_items", [])

            # Frame principal para el pedido
            substatus = pedido.get("shipping", {}).get("substatus_real", "")
            if substatus.lower() == "printed" and incluir_impresos_var.get():
                color_fondo = "#cccccc"  # Gris para impresos
            else:
                color_fondo = "#ffcccc"  # Rojo por defecto (caso contrario)
            order_date_str = pedido.get("date_created")

            if order_date_str:
                try:
                    # Zona horaria UTC-3
                    zona = timezone(timedelta(hours=-3))
                    now_utc3 = datetime.now(zona)
                    order_date_aware = datetime.fromisoformat(order_date_str.replace('Z', '+00:00'))
                    order_date_utc3 = order_date_aware.astimezone(zona)
                    # Comprobar si es hoy y posterior a las 13:00
                    if order_date_utc3.date() == now_utc3.date() and order_date_utc3.hour >= 13:
                        color_fondo = "#d1ffd1"  # Verde
                except (ValueError, TypeError) as e:
                    print(f"Error al procesar fecha del pedido {pedido_id}: {e}")

            pedido_frame = tk.Frame(scroll_frame, bg=color_fondo, padx=2, pady=2, bd=2, relief="groove")
            fila_widgets.append(pedido_frame)
            pedido_frame.pack(fill="x", pady=4, padx=4)

            # Número de pedido (Entry de solo lectura + botón copiar)
            pedido_id_entry = tk.Entry(pedido_frame, font=("Arial", 14, "bold"), bg="white", relief="flat", readonlybackground="white", width=20)
            pedido_id_entry.insert(0, f"{pedido_id}")
            pedido_id_entry.config(state="readonly")
            pedido_id_entry.pack(side="left", padx=(4,2), pady=(2,0))
            def copiar_pedido_id(pid=pedido_id):
                ventana.clipboard_clear()
                ventana.clipboard_append(str(pid))
            btn_copiar_pedido = tk.Button(pedido_frame, text="Copiar", command=copiar_pedido_id, font=("Arial", 10), bg="#e0e0e0")
            btn_copiar_pedido.pack(side="left", pady=(2,0))

            # Substatus a la derecha del pedido_frame (solo una vez por pedido)
            substatus_label = tk.Label(pedido_frame, text=f"Substatus: {substatus}", bg=color_fondo, fg="black", anchor="e", font=("Arial", 10, "bold"))
            substatus_label.pack(side="right", padx=10)

            # Frame para los artículos
            for idx_item, item in enumerate(order_items):
                articulo_frame = tk.Frame(pedido_frame, bg=color_fondo)
                articulo_frame.pack(fill="x", padx=10, pady=1)

                # Nombre del artículo (Entry de solo lectura + botón copiar)
                nombre = nombre_completo(item)
                nombre_entry = tk.Entry(articulo_frame, font=("Arial", 13), bg=color_fondo, relief="flat", readonlybackground=color_fondo, width=60)
                nombre_entry.insert(0, nombre)
                nombre_entry.config(state="readonly")
                nombre_entry.pack(side="left", padx=(0,2), fill="x", expand=True)
                
                # --- Mostrar shipping_id al lado del artículo ---
                shipping_id = pedido.get("shipping", {}).get("id", "")
                tk.Label(articulo_frame, text=f"Shipping ID: {shipping_id}", bg=color_fondo, fg="darkgreen", font=("Arial", 10, "bold")).pack(side="left", padx=(5,5))
                
                # Guardar shipping_id en el item para uso posterior en impresión
                item["_shipping_id_tabla"] = shipping_id
                
                def copiar_nombre_articulo(n=nombre):
                    ventana.clipboard_clear()
                    ventana.clipboard_append(str(n))
                btn_copiar_nombre = tk.Button(articulo_frame, text="Copiar", command=copiar_nombre_articulo, font=("Arial", 11), bg="#e0e0e0")
                btn_copiar_nombre.pack(side="left", padx=(5,5))

                # --- Botón FALLADO ---
                def marcar_fallado(pedido=pedido):
                    try:
                        url = "http://190.211.201.217:5000/cancel"
                        motivo = "FALLADO"
                        pedido_id = pedido["id"]
                        pack_id = pedido.get("pack_id")
                        body = {"order_id": int(pedido_id), "reason": motivo, "mode": "second"}
                        r = requests.post(url, json=body, timeout=10)
                        r.raise_for_status()
                        messagebox.showinfo("Artículo fallado", f"Notificado como fallado. Respuesta: {r.json()}")
                        btn_fallado.config(state="disabled")
                    except Exception as e:
                        messagebox.showerror("Error al notificar fallado", f"Error: {e}")
                btn_fallado = tk.Button(articulo_frame, text="FALLADO", command=marcar_fallado, bg="#ff6666", fg="white", font=("Arial", 11, "bold"))
                btn_fallado.pack(side="left", padx=(5,5))

                # --- Mostrar PEDIDO ID y PACK ID como en prueba-mati.py ---
                # Obtiene el pedido más reciente y muestra pedido_id y pack_id
                # (esto es solo ejemplo, puedes adaptarlo a tu lógica de selección de pedido)
                tok, seller = token, seller_id
                url = (f"https://api.mercadolibre.com/orders/search?"
                       f"seller={seller}&sort=date_desc&limit=50")
                r = requests.get(url, headers={"Authorization": f"Bearer {tok}"}, timeout=10)
                r.raise_for_status()
                data = r.json().get("results", [])
                if data:
                    pedido = data[0]
                    pedido_id = pedido["id"]
                    pack_id  = pedido.get("pack_id")
                    fecha    = pedido["date_created"]

                cantidad = item.get("quantity", 1)
                sku = obtener_sku(item)
                info_text = f"Cant: {cantidad} | SKU: {sku}"
                tk.Label(articulo_frame, text=info_text, bg=color_fondo, anchor="w", font=("Arial", 11)).pack(side="left", fill="x", expand=True)

                # Nota (si existe)
                if nota:
                    tk.Label(articulo_frame, text=f"Nota: {nota}", bg=color_fondo, fg="blue", anchor="e", font=("Arial", 10, "italic")).pack(side="left", padx=10)

        actualizar_resumen()

    btn_actualizar.config(command=actualizar_tabla)

    # --- Controls ---
    controls_frame = tk.Frame(ventana)
    controls_frame.pack(side="bottom", fill="x", padx=10, pady=5)

    # Name search
    name_frame = tk.Frame(controls_frame)
    name_frame.pack(fill="x", pady=2)
    tk.Label(name_frame, text="Buscar por Nombre:", width=20, anchor='w').pack(side="left")
    name_entry = tk.Entry(name_frame)
    name_entry.pack(side="left", fill="x", expand=True, padx=5)
    name_button = tk.Button(name_frame, text="Buscar")
    name_button.pack(side="left")

    # Pickear button
    pickear_button = tk.Button(controls_frame, text="Pickear")
    pickear_button.pack(pady=10)

    # --- Canvas and Scrollbar for the list ---
    main_content_frame = tk.Frame(ventana)
    main_content_frame.pack(side="left", fill="both", expand=True)

    canvas_frame = tk.Frame(main_content_frame)
    canvas_frame.pack(side="left", fill="both", expand=True)
    canvas = tk.Canvas(canvas_frame)
    scrollbar = tk.Scrollbar(canvas_frame, orient="vertical", command=canvas.yview)
    hscrollbar = tk.Scrollbar(canvas_frame, orient="horizontal", command=canvas.xview)
    scroll_frame = tk.Frame(canvas)

    scroll_frame.bind("<Configure>", lambda e: canvas.configure(scrollregion=canvas.bbox("all")))
    canvas.create_window((0, 0), window=scroll_frame, anchor="nw")
    canvas.configure(yscrollcommand=scrollbar.set, xscrollcommand=hscrollbar.set)

    canvas.pack(side="left", fill="both", expand=True)
    scrollbar.pack(side="right", fill="y")
    hscrollbar.pack(side="bottom", fill="x")

    def buscar_por_nombre(event=None):
        search_term = name_entry.get().strip().lower()
        if not search_term:
            return

        for i, p in enumerate(pedidos_filtrados):
            nombres_items = [nombre_completo(item).lower() for item in p["order_items"]]
            if any(search_term in nombre for nombre in nombres_items):
                ventana.update_idletasks()
                canvas.yview_moveto(fila_widgets[i].winfo_y() / scroll_frame.winfo_height())
                return 
        
        messagebox.showinfo("No encontrado", f"No se encontró un item con el nombre '{name_entry.get()}'.")

    name_button.config(command=buscar_por_nombre)
    name_entry.bind('<Return>', buscar_por_nombre)

    def iniciar_proceso_pickeo(pedidos_a_pickear=None, saltados=None):
        # --- LOGGING SETUP ---
        now = datetime.now()
        year_str = now.strftime("%Y")
        month_str = now.strftime("%m")
        log_dir = os.path.join(DROPBOX_LOG_PATH, year_str, month_str)
        os.makedirs(log_dir, exist_ok=True)

        date_str = now.strftime("%d-%m-%Y")
        base_log_path = os.path.join(log_dir, f"{date_str}.txt")
        session_log_path = base_log_path
        
        counter = 2
        while os.path.exists(session_log_path):
            session_log_path = os.path.join(log_dir, f"{date_str}_{counter}.txt")
            counter += 1
        
        print(f"Iniciando sesión de pickeo. Log en: {session_log_path}")
        # --- END LOGGING SETUP ---

        # --- NUEVO: Control de pedidos saltados ---
        if saltados is None:
            saltados = set()
        if pedidos_a_pickear is None:
            pedidos_a_pickear = list(range(len(pedidos_filtrados)))

        # --- CAMBIO: Expandir pendientes_global por unidad ---
        pendientes_global = []  # (idx_pedido, idx_item, item, unidad_idx)
        pickeados = set()  # (idx_pedido, idx_item, unidad_idx)
        for idx_pedido in pedidos_a_pickear:
            pedido = pedidos_filtrados[idx_pedido]
            venta_id = pedido.get("pack_id") or pedido.get("id")
            if venta_id in saltados:
                continue
            for idx_item, item in enumerate(pedido["order_items"]):
                cantidad = item.get("quantity", 1)
                for unidad_idx in range(cantidad):
                    pendientes_global.append((idx_pedido, idx_item, item, unidad_idx))
        barcodes_por_pedido = {i: [] for i in pedidos_a_pickear}
        pickeados_por_pedido = {i: set() for i in pedidos_a_pickear}
        total_items_por_pedido = {i: sum(item.get("quantity", 1) for item in get_pedido_by_venta_id(get_venta_id(pedidos_filtrados[i]))["order_items"]) for i in pedidos_a_pickear}

        def normalizar_codigo(c):
            return c.replace("-", "").replace("_", "").replace(" ", "").upper() if c else ""

        def modo_libre_pickeo():
            if not pendientes_global:
                # --- Al terminar, preguntar si hay saltados ---
                if saltados:
                    if messagebox.askyesno("Pedidos saltados", "¿Desea pickear los pedidos que fueron saltados?"):
                        # Reiniciar pickeo solo con los pedidos saltados
                        saltados_a_pickear = list()
                        for idx_pedido, pedido in enumerate(pedidos_filtrados):
                            venta_id = pedido.get("pack_id") or pedido.get("id")
                            if venta_id in saltados:
                                saltados_a_pickear.append(idx_pedido)
                        # Limpiar saltados para evitar bucles
                        saltados.clear()
                        iniciar_proceso_pickeo(pedidos_a_pickear=saltados_a_pickear, saltados=saltados)
                        return
                messagebox.showinfo("Pickeo terminado", "No quedan más artículos para pickear.")
                return
            picker_window = tk.Toplevel(ventana)
            picker_window.title("Pickeo Libre")
            picker_window.geometry("400x200")
            tk.Label(picker_window, text="Escanee el código de barra de cualquier artículo pendiente:").pack(pady=10)
            sku_entry = tk.Entry(picker_window)
            sku_entry.pack(pady=10)
            sku_entry.focus()
            
            def obtener_codigo_ml_desde_barra(codigo_barra):
                conn_str = (
                    "DRIVER={ODBC Driver 17 for SQL Server};"
                    "SERVER=ranchoaspen\\zoo2025;"
                    "DATABASE=dragonfish_deposito;"
                    "Trusted_Connection=yes;"
                )
                query = '''
                SELECT 
                    RTRIM(equi.CCOLOR) AS CODIGO_COLOR,
                    RTRIM(equi.CTALLE) AS CODIGO_TALLE,
                    RTRIM(equi.CARTICUL) AS CODIGO_ARTICULO,
                    RTRIM(equi.CCODIGO) AS CODIGO_BARRA,
                    RTRIM(c_art.ARTDES) AS ARTDES
                FROM 
                    DRAGONFISH_DEPOSITO.ZooLogic.EQUI as equi
                    LEFT JOIN DRAGONFISH_DEPOSITO.ZooLogic.ART AS c_art 
                        ON equi.CARTICUL = c_art.ARTCOD
                WHERE RTRIM(equi.CCODIGO) = ?
                '''
                with pyodbc.connect(conn_str) as conn:
                    cursor = conn.cursor()
                    cursor.execute(query, (codigo_barra,))
                    row = cursor.fetchone()
                    if row:
                        codigo_ml = f"{row.CODIGO_ARTICULO}-{row.CODIGO_COLOR}-{row.CODIGO_TALLE}"
                        return codigo_ml, row.CODIGO_BARRA
                return None, None

            def get_venta_id(pedido):
                return pedido.get("pack_id") or pedido.get("id")

            def pickear_secuencial_venta(venta_id):
                # Buscar todos los (idx_pedido, idx_item, item, unidad_idx) de la misma venta
                pendientes_venta = []
                for idx_pedido, pedido in enumerate(pedidos_filtrados):
                    if get_venta_id(pedido) == venta_id:
                        for idx_item, item in enumerate(pedido["order_items"]):
                            cantidad = item.get("quantity", 1)
                            for unidad_idx in range(cantidad):
                                if (idx_pedido, idx_item, unidad_idx) not in pickeados:
                                    pendientes_venta.append((idx_pedido, idx_item, item, unidad_idx))
                if pendientes_venta:
                    # Pickear el primero pendiente
                    idx_pedido, idx_item, item, unidad_idx = pendientes_venta[0]
                    pickear_item_venta(venta_id, idx_pedido, idx_item, item, unidad_idx)
                    return
                # Si no quedan pendientes, imprimir
                # Buscar shipping_id y pedido correcto de la venta
                pedido_actual = None
                shipping_id = None
                for pedido_candidato in pedidos_filtrados:
                    if get_venta_id(pedido_candidato) == venta_id:
                        pedido_actual = pedido_candidato
                        shipping_id = pedido_actual.get("shipping", {}).get("id")
                        break
                if shipping_id and pedido_actual:
                    zpl_content = descargar_etiquetas_zpl([shipping_id], token)
                    # Control de pack_id antes de imprimir
                    pack_id_esperado = pedido_actual.get("pack_id")
                    pack_id_a_imprimir = pedido_actual.get("pack_id")
                    if pack_id_esperado != pack_id_a_imprimir:
                        print(f"[ERROR] El pack_id de la etiqueta ({pack_id_a_imprimir}) no coincide con el del pedido ({pack_id_esperado}). No se imprimirá la etiqueta.")
                    else:
                        if zpl_content:
                            imprimir_zebra(zpl_content)
                        # --- Enviar movimiento a Dragonfish por cada artículo pickeado ---
                        for idx2, item2 in enumerate(pedido_actual["order_items"]):
                            cantidad2 = item2.get("quantity", 1)
                            sku2 = obtener_sku(item2)
                            for (cant, codigo_barra_pick) in barcodes_por_pedido.get(idx_pedido, []):
                                datos_articulo = obtener_datos_articulo_para_api(codigo_barra_pick)
                                if datos_articulo:
                                    enviar_movimiento_stock_dragonfish(pedido_actual.get("pack_id") or pedido_actual.get("id"), codigo_barra_pick, cant, datos_articulo)
                        actualizar_tabla()
                        for i, pedido2 in enumerate(pedidos_filtrados):
                            if get_venta_id(pedido2) == venta_id and i < len(fila_widgets):
                                fila_widgets[i].config(bg="violet")
                        actualizar_substatus_envio(shipping_id, token)
                messagebox.showinfo("Venta completa", "¡Todos los artículos de esta venta han sido pickeados e impresos!")
                modo_libre_pickeo()

            def pickear_item_venta(venta_id, idx_pedido, idx_item, item, unidad_idx):
                pedido = pedidos_filtrados[idx_pedido]
                pedido_id = get_venta_id(pedido)
                # Usar el shipping_id guardado en el item (de la tabla)
                shipping_id_tabla = item.get("_shipping_id_tabla") or pedido.get("shipping", {}).get("id")
                nombre = nombre_completo(item)
                cantidad = item.get("quantity", 1)
                seller_custom_field = item["item"].get("seller_custom_field", "")
                sku = obtener_sku(item)
                item_id = item["item"].get("id")
                variation_id = item["item"].get("variation_id")
                picture_id = None
                if variation_id:
                    url = f'https://api.mercadolibre.com/items/{item_id}'
                    headers = {"Authorization": f"Bearer {token}"}
                    res = requests.get(url, headers=headers)
                    if res.ok:
                        item_data = res.json()
                        for var in item_data.get("variations", []):
                            if var.get("id") == variation_id:
                                picture_ids = var.get("picture_ids", [])
                                if picture_ids:
                                    picture_id = picture_ids[0]
                confirm_window = tk.Toplevel(ventana)
                confirm_window.title("Pickear artículo de la venta")
                confirm_window.geometry("350x400")
                tk.Label(confirm_window, text=f"Venta: {pedido_id}", font=("Arial", 12, "bold")).pack(pady=5)
                tk.Label(confirm_window, text=f"Artículo: {nombre}", font=("Arial", 11)).pack(pady=5)
                tk.Label(confirm_window, text=f"Cantidad: {cantidad} | SKU: {sku}").pack(pady=5)
                tk.Label(confirm_window, text=f"Unidad: {unidad_idx+1} de {cantidad}").pack(pady=5)
                tk.Label(confirm_window, text="Escanee el código de barra de este artículo:").pack(pady=10)
                sku_entry2 = tk.Entry(confirm_window)
                sku_entry2.pack(pady=10)
                sku_entry2.focus()
                def confirmar_pickeo(item_local=item, idx_pedido_local=idx_pedido, idx_item_local=idx_item, unidad_idx_local=unidad_idx):
                    codigo_barra = sku_entry2.get().strip()
                    if not codigo_barra:
                        return
                    codigo_ml, codigo_barra_real = obtener_codigo_ml_desde_barra(codigo_barra)
                    sku_actual = obtener_sku(item_local)
                    if not codigo_ml or normalizar_codigo(sku_actual) != normalizar_codigo(codigo_ml):
                        messagebox.showerror("No coincide", f"El código escaneado no corresponde a este artículo.", parent=confirm_window)
                        return
                    # Escribir en el log de la sesión (acumular por código de barra)
                    try:
                        with open(session_log_path, 'a', encoding='utf-8') as f:
                            f.write(f"1+{codigo_barra_real}\n")
                    except Exception as e:
                        print(f"Error al escribir en el log de sesión: {e}")
                        messagebox.showerror("Error de Log", f"No se pudo escribir en el archivo de log:\n{e}", parent=confirm_window)
                    pickeados.add((idx_pedido_local, idx_item_local, unidad_idx_local))
                    pickeados_por_pedido[idx_pedido_local].add((idx_item_local, unidad_idx_local))
                    barcodes_por_pedido[idx_pedido_local].append((1, codigo_barra_real))
                    actualizar_resumen()
                    confirm_window.destroy()
                    # Buscar si quedan unidades pendientes en la venta
                    pendientes_venta = []
                    for i, pedido2 in enumerate(pedidos_filtrados):
                        if get_venta_id(pedido2) == venta_id:
                            for j, item2 in enumerate(pedido2["order_items"]):
                                cantidad2 = item2.get("quantity", 1)
                                for unidad_idx2 in range(cantidad2):
                                    if (i, j, unidad_idx2) not in pickeados:
                                        pendientes_venta.append((i, j, item2, unidad_idx2))
                    if pendientes_venta:
                        siguiente = pendientes_venta[0]
                        pickear_item_venta(venta_id, siguiente[0], siguiente[1], siguiente[2], siguiente[3])
                    else:
                        # Imprimir etiqueta y volver a modo libre
                        pedido_actual = None
                        shipping_id = None
                        for pedido2 in pedidos_filtrados:
                            if get_venta_id(pedido2) == venta_id:
                                pedido_actual = pedido2
                                shipping_id = pedido_actual.get("shipping", {}).get("id")
                                break
                        if shipping_id and pedido_actual:
                            zpl_content = descargar_etiquetas_zpl([shipping_id], token)
                            # Control de pack_id antes de imprimir
                            pack_id_esperado = pedido_actual.get("pack_id")
                            pack_id_a_imprimir = pedido_actual.get("pack_id")
                            if pack_id_esperado != pack_id_a_imprimir:
                                print(f"[ERROR] El pack_id de la etiqueta ({pack_id_a_imprimir}) no coincide con el del pedido ({pack_id_esperado}). No se imprimirá la etiqueta.")
                            else:
                                if zpl_content:
                                    imprimir_zebra(zpl_content)
                                # --- Enviar movimiento a Dragonfish por cada artículo pickeado ---
                                for idx2, item2 in enumerate(pedido_actual["order_items"]):
                                    cantidad2 = item2.get("quantity", 1)
                                    sku2 = obtener_sku(item2)
                                    for (cant, codigo_barra_pick) in barcodes_por_pedido.get(idx_pedido, []):
                                        datos_articulo = obtener_datos_articulo_para_api(codigo_barra_pick)
                                        if datos_articulo:
                                            enviar_movimiento_stock_dragonfish(pedido_id, codigo_barra_pick, cant, datos_articulo)
                                actualizar_tabla()
                                for i, pedido2 in enumerate(pedidos_filtrados):
                                    if get_venta_id(pedido2) == venta_id and i < len(fila_widgets):
                                        fila_widgets[i].config(bg="violet")
                                actualizar_substatus_envio(shipping_id, token)
                        messagebox.showinfo("Venta completa", "¡Todos los artículos de esta venta han sido pickeados e impresos!")
                        modo_libre_pickeo()
                tk.Button(confirm_window, text="Pickear", command=confirmar_pickeo, bg="lightgreen").pack(pady=10)
                sku_entry2.bind('<Return>', lambda e: confirmar_pickeo())
                def saltar_pedido():
                    saltados.add(venta_id)
                    nonlocal pendientes_global
                    pendientes_global = [x for x in pendientes_global if get_venta_id(get_pedido_by_venta_id(get_venta_id(pedidos_filtrados[x[0]]))) != venta_id]
                    confirm_window.destroy()
                    modo_libre_pickeo()
                tk.Button(confirm_window, text="Saltar este pedido", command=saltar_pedido, bg="#ffb366").pack(pady=10)
                confirm_window.protocol("WM_DELETE_WINDOW", confirm_window.destroy)

            def buscar_y_pickear():
                codigo_barra = sku_entry.get().strip()
                if not codigo_barra:
                    return
                codigo_ml, codigo_barra_real = obtener_codigo_ml_desde_barra(codigo_barra)
                if not codigo_ml:
                    messagebox.showerror("No encontrado", f"Código de barra {codigo_barra} no encontrado en la base SQL.", parent=picker_window)
                    return
                for idx, (idx_pedido, idx_item, item, unidad_idx) in enumerate(pendientes_global):
                    sku = obtener_sku(item)
                    if normalizar_codigo(sku) == normalizar_codigo(codigo_ml) and (idx_pedido, idx_item, unidad_idx) not in pickeados:
                        pedido = pedidos_filtrados[idx_pedido]  # USAR SIEMPRE EL OBJETO DIRECTO
                        pedido_id = get_venta_id(pedido)
                        total_items_venta = sum(item2.get("quantity", 1) for p in pedidos_filtrados if get_venta_id(p) == pedido_id for item2 in p["order_items"])
                        pickeados_venta = sum(1 for p_idx, i_idx, u_idx in pickeados if get_venta_id(pedidos_filtrados[p_idx]) == pedido_id)
                        if total_items_venta > 1:
                            picker_window.destroy()
                            pickear_item_venta(pedido_id, idx_pedido, idx_item, item, unidad_idx)
                            return
                        # Si es un solo artículo en la venta, pickear y permitir imprimir
                        nombre = nombre_completo(item)
                        cantidad = item.get("quantity", 1)
                        item_id = item["item"].get("id")
                        variation_id = item["item"].get("variation_id")
                        picture_id = None
                        if variation_id:
                            url = f'https://api.mercadolibre.com/items/{item_id}'
                            headers = {"Authorization": f"Bearer {token}"}
                            res = requests.get(url, headers=headers)
                            if res.ok:
                                item_data = res.json()
                                for var in item_data.get("variations", []):
                                    if var.get("id") == variation_id:
                                        picture_ids = var.get("picture_ids", [])
                                        if picture_ids:
                                            picture_id = picture_ids[0]
                        confirm_window = tk.Toplevel(picker_window)
                        confirm_window.title("Confirmar impresión")
                        confirm_window.geometry("350x350")
                        tk.Label(confirm_window, text=f"Venta: {pedido_id}", font=("Arial", 12, "bold")).pack(pady=5)
                        tk.Label(confirm_window, text=f"Artículo: {nombre}", font=("Arial", 11)).pack(pady=5)
                        tk.Label(confirm_window, text=f"Cantidad: {cantidad} | SKU: {sku}").pack(pady=5)
                        tk.Label(confirm_window, text=f"Unidad: {unidad_idx+1} de {cantidad}").pack(pady=5)
                        def confirmar():
                            confirm_window.destroy()
                            try:
                                with open(session_log_path, 'a', encoding='utf-8') as f:
                                    f.write(f"1+{codigo_barra_real}\n")
                            except Exception as e:
                                print(f"Error al escribir en el log de sesión: {e}")
                                messagebox.showerror("Error de Log", f"No se pudo escribir en el archivo de log:\n{e}", parent=confirm_window)
                            pickeados.add((idx_pedido, idx_item, unidad_idx))
                            pickeados_por_pedido[idx_pedido].add((idx_item, unidad_idx))
                            barcodes_por_pedido[idx_pedido].append((1, codigo_barra_real))
                            actualizar_resumen()
                            del pendientes_global[idx]
                            # Usar el shipping_id guardado en el item (de la tabla)
                            shipping_id_tabla = item.get("_shipping_id_tabla") or pedido.get("shipping", {}).get("id")
                            if shipping_id_tabla:
                                zpl_content = descargar_etiquetas_zpl([shipping_id_tabla], token)
                                # Control de pack_id antes de imprimir
                                pack_id_esperado = pedido.get("pack_id")
                                pack_id_a_imprimir = pedido.get("pack_id")
                                if pack_id_esperado != pack_id_a_imprimir:
                                    print(f"[ERROR] El pack_id de la etiqueta ({pack_id_a_imprimir}) no coincide con el del pedido ({pack_id_esperado}). No se imprimirá la etiqueta.")
                                else:
                                    if zpl_content:
                                        imprimir_zebra(zpl_content)
                                    # --- Enviar movimiento a Dragonfish por cada artículo pickeado ---
                                    for idx2, item2 in enumerate(pedido["order_items"]):
                                        cantidad2 = item2.get("quantity", 1)
                                        sku2 = obtener_sku(item2)
                                        for (cant, codigo_barra_pick) in barcodes_por_pedido.get(idx_pedido, []):
                                            datos_articulo = obtener_datos_articulo_para_api(codigo_barra_pick)
                                            if datos_articulo:
                                                enviar_movimiento_stock_dragonfish(pedido_id, codigo_barra_pick, cant, datos_articulo)
                                    actualizar_tabla()
                                    if idx_pedido < len(fila_widgets):
                                        fila_widgets[idx_pedido].config(bg="violet")
                                    actualizar_substatus_envio(shipping_id_tabla, token)
                            picker_window.destroy()
                            modo_libre_pickeo()
                        btn_confirmar = tk.Button(confirm_window, text="Confirmar e Imprimir", command=confirmar, bg="lightgreen", font=("Arial", 12, "bold"))
                        btn_confirmar.pack(pady=20)
                        confirm_window.protocol("WM_DELETE_WINDOW", confirm_window.destroy)
                        return
                messagebox.showerror("No encontrado", f"El código {codigo_ml} no corresponde a ningún artículo pendiente o ya fue pickeado.", parent=picker_window)
            tk.Button(picker_window, text="Pickear", command=buscar_y_pickear, bg="lightgreen").pack(pady=10)
            sku_entry.bind('<Return>', lambda e: buscar_y_pickear())
            picker_window.protocol("WM_DELETE_WINDOW", lambda: picker_window.destroy())
        modo_libre_pickeo()
    pickear_button.config(command=iniciar_proceso_pickeo)

    def actualizar_resumen():
        resumen = {}
        for pedido in pedidos_filtrados:
            venta_id = pedido.get("pack_id") or pedido.get("id")
            total = len(pedido["order_items"])
            if venta_id not in resumen:
                resumen[venta_id] = 0
            resumen[venta_id] += total
        texto = ""
        for venta_id, total in resumen.items():
            texto += f"Venta {venta_id}: {total} artículos\n"
        if resumen_label:
            resumen_label.config(text=texto)

def fijar_fecha():
    desde = entry_desde.get()
    hasta = entry_hasta.get()
    try:
        dt1 = datetime.strptime(desde, "%d/%m/%Y")
        dt2 = datetime.strptime(hasta, "%d/%m/%Y")
    except:
        messagebox.showerror("Formato incorrecto", "Usar formato DD/MM/YYYY")
        return

    desde_api = dt1.strftime("%Y-%m-%d")
    hasta_api = dt2.strftime("%Y-%m-%d")
    global token, seller_id, pedidos_filtrados
    token, seller_id = obtener_access_token()
    pedidos = listar_pedidos(seller_id, token, desde_api, hasta_api)

    print("Pedidos encontrados en el rango de fechas:")
    pedidos_filtrados.clear()
    # --- Consulta de substatus en paralelo ---
    with ThreadPoolExecutor(max_workers=8) as executor:
        futuros = [executor.submit(obtener_substatus_real_pedido, p, token) for p in pedidos]
        for futuro in as_completed(futuros):
            res = futuro.result()
            if res:
                pedidos_filtrados.append(res)

    if not pedidos_filtrados:
        messagebox.showinfo("Sin resultados", "No se encontraron pedidos con las notas correspondientes y estado 'printed' o 'ready_to_print'.")
    else:
        root.destroy()
        mostrar_tabla_depo(desde, hasta)

def actualizar_substatus_envio(shipping_id, access_token, nuevo_substatus="printed"):
    url = f"https://api.mercadolibre.com/shipments/{shipping_id}"
    headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}
    data = json.dumps({"substatus": nuevo_substatus})
    response = requests.put(url, headers=headers, data=data)
    return response.status_code == 200

# 1. Función auxiliar para obtener el SKU de un item:
def obtener_sku(item):
    # 1. Buscar en seller_sku directo
    sku = item.get('item', {}).get('seller_sku')
    if sku:
        return sku
    # 2. Buscar en attributes
    attributes = item.get('item', {}).get('attributes', [])
    for attr in attributes:
        if attr.get('id') == 'SELLER_SKU' and attr.get('value_name'):
            return attr['value_name']
    # 3. Buscar variation_sku
    sku = item.get('item', {}).get('variation_sku')
    if sku:
        return sku
    # 4. Buscar seller_custom_field
    sku = item.get('item', {}).get('seller_custom_field')
    if sku:
        return sku
    return 'SIN SKU'

def fecha_dragonfish():
    zona = timezone(timedelta(hours=-3))
    ms = int(datetime.now(zona).timestamp() * 1000)
    return f"/Date({ms}-0300)/"

def hora_dragonfish():
    zona = timezone(timedelta(hours=-3))
    return datetime.now(zona).strftime("%H:%M:%S")

# --- NUEVA FUNCIÓN: Consulta SQL extendida para ARTDES ---
def obtener_datos_articulo_para_api(codigo_barra):
    conn_str = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=ranchoaspen\\zoo2025;"
        "DATABASE=dragonfish_deposito;"
        "Trusted_Connection=yes;"
    )
    query = '''
    SELECT 
        RTRIM(equi.CCOLOR) AS CODIGO_COLOR,
        RTRIM(equi.CTALLE) AS CODIGO_TALLE,
        RTRIM(equi.CARTICUL) AS CODIGO_ARTICULO,
        RTRIM(equi.CCODIGO) AS CODIGO_BARRA,
        RTRIM(c_art.ARTDES) AS ARTDES
    FROM 
        DRAGONFISH_DEPOSITO.ZooLogic.EQUI as equi
        LEFT JOIN DRAGONFISH_DEPOSITO.ZooLogic.ART AS c_art 
            ON equi.CARTICUL = c_art.ARTCOD
    WHERE RTRIM(equi.CCODIGO) = ?
    '''
    with pyodbc.connect(conn_str) as conn:
        cursor = conn.cursor()
        cursor.execute(query, (codigo_barra,))
        row = cursor.fetchone()
        if row:
            return {
                'CODIGO_ARTICULO': row.CODIGO_ARTICULO,
                'CODIGO_COLOR': row.CODIGO_COLOR,
                'CODIGO_TALLE': row.CODIGO_TALLE,
                'CODIGO_BARRA': row.CODIGO_BARRA,
                'ARTDES': row.ARTDES
            }
    return None

# --- NUEVA FUNCIÓN: Enviar movimiento de stock a Dragonfish ---
def enviar_movimiento_stock_dragonfish(pedido_id, codigo_barra, cantidad, datos_articulo):
    url = "http://190.211.201.217:8009/api.Dragonfish/Movimientodestock/"
    headers = {
        "accept": "application/json",
        "IdCliente": "PRUEBA-WEB",
        "Authorization": "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJleHAiOjE4MTQzNzg3MDYsInVzdWFyaW8iOiJBRE1JTiIsInBhc3N3b3JkIjoiMDE5NGRkMGEyYjA2Yzc4Yzc5YmUxZThjMDQ3ZmFkNTgyZTM2NzJlMzQ0NWFlYzRlODMwMDFiNDdlNGE4MWQwMyJ9.foX4tqZf_tXfwGVtOC-tY4iDy9ebPtTSKePJ8a1nZWQ",
        "Content-Type": "application/json",
        "BaseDeDatos": "DEPOSITO"
    }
    fecha = fecha_dragonfish()
    hora = hora_dragonfish()
    now = datetime.now()
    fecha_hora_str = now.strftime("%d%m%Y%H%M%S")
    codigo = f"API{codigo_barra}{fecha_hora_str}"
    body = {
        "OrigenDestino": "WOO",
        "Tipo": 2,
        "Motivo": "API",
        "vendedor": "API",
        "Remito": "-",
        "CompAfec": [],
        "Fecha": fecha,
        "Observacion": f"MELI API {pedido_id}",
        "MovimientoDetalle": [
            {
                "Articulo": datos_articulo['CODIGO_BARRA'],
                "ArticuloDetalle": datos_articulo['ARTDES'],
                "Color": datos_articulo['CODIGO_COLOR'],
                "Talle": datos_articulo['CODIGO_TALLE'],
                "Cantidad": cantidad,
                "NroItem": 1
            }
        ],
        "InformacionAdicional": {
            "FechaAltaFW": fecha,
            "HoraAltaFW": hora,
            "EstadoTransferencia": "PENDIENTE",
            "BaseDeDatosAltaFW": "DEPOSITO",
            "BaseDeDatosModificacionFW": "DEPOSITO",
            "SerieAltaFW": "901224",
            "SerieModificacionFW": "901224",
            "UsuarioAltaFW": "API",
            "UsuarioModificacionFW": "API"
        }
    }
    try:
        print("URL Dragonfish:")
        print(url)
        print("Headers enviados a Dragonfish:")
        print(json.dumps(headers, indent=2, ensure_ascii=False))
        print("Payload enviado a Dragonfish:")
        print(json.dumps(body, indent=2, ensure_ascii=False))
        resp = requests.post(url, headers=headers, data=json.dumps(body))
        print(f"Respuesta Dragonfish: {resp.status_code} {resp.text}")
        return resp.status_code == 200
    except Exception as e:
        print(f"Error enviando movimiento a Dragonfish: {e}")
        return False

def get_venta_id(pedido):
    return pedido.get("pack_id") or pedido.get("id")

def get_pedido_by_venta_id(venta_id):
    for pedido in pedidos_filtrados:
        if (pedido.get("pack_id") or pedido.get("id")) == venta_id:
            return pedido
    return None

def get_shipping_id_by_order_id(order_id):
    for pedido in pedidos_filtrados:
        if pedido.get("id") == order_id:
            return pedido.get("shipping", {}).get("id")
    return None

# === VENTANA PRINCIPAL ===
root = tk.Tk()
root.title("Ingresar Fechas")

tk.Label(root, text="Fecha Desde (DD/MM/YYYY):").pack()
entry_desde = tk.Entry(root)
entry_desde.pack()

tk.Label(root, text="Fecha Hasta (DD/MM/YYYY):").pack()
entry_hasta = tk.Entry(root)
entry_hasta.pack()

tk.Button(root, text="FIJAR FECHA", command=fijar_fecha).pack(pady=10)

root.mainloop()
