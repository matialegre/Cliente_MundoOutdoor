"""Fase 2 – GUI simplificada en tabla usando ttkbootstrap.

Este módulo crea `AppV2`, una ventana alternativa que muestra los pedidos
(filtrados) en una tabla con cuatro columnas y botones INFO / FALLADO.
Mantiene toda la lógica de backend implementada en la Fase 1 sin modificar
ningún servicio. Solo cambia la presentación.
"""
from __future__ import annotations

# Permitir ejecución directa añadiendo el directorio raíz al sys.path
import sys, pathlib
ROOT_DIR = pathlib.Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from datetime import datetime, timedelta
import tkinter as tk
from tkinter import ttk
import ttkbootstrap as tb
from ttkbootstrap.constants import *
import ttkbootstrap.dialogs as dialogs
import win32print  # type: ignore

from typing import List, Dict, Any

from services.picker_service import PickerService
from services import print_service, fail_service
from models.gui_state import GuiState
from services.refresh_service import OrderRefresher
from printing.zpl_printer import print_zpl
from utils import config

__all__ = ["launch_gui_v2", "AppV2"]

DEFAULT_MOTIVOS = ["Manchado", "Roto", "No está"]
KEYWORDS_NOTE = ['DEPO', 'MUNDOAL', 'MTGBBL', 'BBPS', 'MONBAHIA', 'MTGBBPS']


class AppV2(tb.Window):
    COLS = ("art", "talle", "color", "cant")

    def __init__(self) -> None:
        super().__init__(title="Cliente Matías – Tabla", themename="darkly")
        self.geometry("1024x700")

        # --- servicios / estado ---
        self.picker = PickerService()
        self.state = GuiState()
        self.refresher = OrderRefresher(self.picker, self.state)
        self._refresher_started = False

        self._current_ids: List[str] = []  # track to evitar repintar
        self._build_widgets()
        self.after(1000, self.poll_state)

    # ------------------------------------------------------------------
    # Construcción de GUI
    # ------------------------------------------------------------------
    def _build_widgets(self) -> None:
        frm_top = tb.Frame(self, padding=10)
        frm_top.pack(fill=X)

        today = datetime.now()
        yesterday = today - timedelta(days=1)
        self.var_from = tk.StringVar(value=yesterday.strftime("%d/%m/%Y"))
        self.var_to = tk.StringVar(value=today.strftime("%d/%m/%Y"))
        self.var_include_printed = tk.BooleanVar(value=False)
        self.var_until_13 = tk.BooleanVar(value=False)

        tb.Label(frm_top, text="Desde", bootstyle=SECONDARY).grid(row=0, column=0, padx=2)
        tb.Entry(frm_top, textvariable=self.var_from, width=12).grid(row=0, column=1, padx=2)
        tb.Label(frm_top, text="hasta", bootstyle=SECONDARY).grid(row=0, column=2, padx=2)
        tb.Entry(frm_top, textvariable=self.var_to, width=12).grid(row=0, column=3, padx=2)
        tb.Checkbutton(frm_top, text="Incluir impresos", variable=self.var_include_printed).grid(row=0, column=4, padx=6)
        tb.Checkbutton(frm_top, text="Pedidos hasta 13 hs", variable=self.var_until_13).grid(row=0, column=5, padx=6)

        btn_load = tb.Button(frm_top, text="Cargar", command=self.load_orders, bootstyle=PRIMARY)
        btn_load.grid(row=0, column=6, padx=6)

        btn_print = tb.Button(frm_top, text="Imprimir lista", command=self.on_print_list, bootstyle=INFO)
        btn_print.grid(row=0, column=7, padx=6)

        # --- Menú Más opciones ---
        more_menu = tk.Menu(self, tearoff=0)
        more_menu.add_command(label="Elegir impresora", command=self.choose_printer)
        more_menu.add_command(label="Test impresora", command=self.test_printer)
        mb_more = tb.Menubutton(frm_top, text="Más opciones", menu=more_menu, bootstyle=SECONDARY)
        mb_more.grid(row=0, column=8, padx=6)

        # --- Historial ---
        mb_hist_menu = tk.Menu(self, tearoff=0)
        mb_hist_menu.add_command(label="Ver historial", command=self.open_historial)
        mb_hist = tb.Menubutton(frm_top, text="Historial", menu=mb_hist_menu, bootstyle=SECONDARY)
        mb_hist.grid(row=0, column=9, padx=6)

        # --- Lista de pedidos estilo GUI 1 (Canvas + Frames) ---
        frm_orders = tb.Frame(self, padding=20)
        frm_orders.pack(fill=BOTH, expand=YES)
        
        # Canvas con scroll para los pedidos
        self.canvas = tk.Canvas(frm_orders, bd=0, highlightthickness=0, bg='#2b2b2b')
        vscroll = ttk.Scrollbar(frm_orders, orient="vertical", command=self.canvas.yview)
        self.inner_frame = tk.Frame(self.canvas, bg='#2b2b2b')
        
        # Configurar el scroll
        self.canvas.configure(yscrollcommand=vscroll.set, bg='#2b2b2b')
        vscroll.pack(side="right", fill="y")
        self.canvas.pack(side="left", fill="both", expand=True)
        self.canvas.create_window((0, 0), window=self.inner_frame, anchor="nw", tags=("inner",))
        
        # Configurar el scroll con el mouse
        def _on_mousewheel(event):
            self.canvas.yview_scroll(int(-1*(event.delta/120)), "units")
        
        def _on_frame_configure(event=None):
            self.canvas.configure(scrollregion=self.canvas.bbox("all"))
            
        self.inner_frame.bind("<Configure>", _on_frame_configure)
        self.canvas.bind("<Configure>", lambda e: self.canvas.itemconfig("inner", width=e.width))
        self.canvas.bind_all("<MouseWheel>", _on_mousewheel)
        
        # Lista para mantener referencias a los frames de pedidos
        self.order_frames = []

        # Botones de acción principales arriba
        self.btn_info = tb.Button(frm_top, text="INFO", command=self.on_info, state=DISABLED)
        self.btn_info.grid(row=0, column=11, padx=4)
        self.btn_fallado = tb.Button(frm_top, text="FALLADO", bootstyle=DANGER, command=self.on_fallado, state=DISABLED)
        self.btn_fallado.grid(row=0, column=12, padx=4)

        # Marco inferior con botón Pickear centrado y prominente
        action_frame = tb.Frame(self, padding=15)
        action_frame.pack(fill=X)
        
        # Crear un frame interno para centrar el botón
        center_frame = tb.Frame(action_frame)
        center_frame.pack(expand=True)
        
        # Botón Pickear más grande y prominente
        btn_pick = tb.Button(
            center_frame, 
            text="📦 PICKEAR", 
            bootstyle="warning-outline",
            command=self.open_pick_window, 
            state=DISABLED,
            width=20
        )
        btn_pick.pack(pady=10)
        self.btn_pick = btn_pick

    # ------------------------------------------------------------------
    # Refresh + render
    # --------------------------------------------------------------------------------------------------------------------
    # Filtros
    # ------------------------------------------------------------------
    def _filter_orders(self, orders):
        try:
            date_from = datetime.strptime(self.var_from.get(), "%d/%m/%Y").date()
            date_to = datetime.strptime(self.var_to.get(), "%d/%m/%Y").date()
        except ValueError:
            date_from = date_to = None

        allow_printed = self.var_include_printed.get()
        until_13 = self.var_until_13.get()
        filtered = []
        for o in orders:
            # fecha
            try:
                ts = datetime.fromisoformat(o.date_created)
            except Exception:
                ts = None
            if ts and date_from and date_to and not (date_from <= ts.date() <= date_to):
                continue
            if until_13 and ts and ts.time() >= datetime.strptime("13:00", "%H:%M").time():
                continue
            # palabras clave en nota - SOLO DEPO/DEPOSITO para este depósito
            note_up = (o.notes or '').upper()
            # Solo procesar pedidos que contengan DEPO o DEPOSITO (no otros depósitos)
            if not ('DEPO' in note_up or 'DEPOSITO' in note_up):
                continue
            sub = (o.shipping_substatus or '').lower()
            if sub == 'ready_to_print':
                filtered.append(o)
                continue
            if allow_printed and sub == 'printed':
                filtered.append(o)
                continue
        return filtered

    # ------------------------------------------------------------------
    def poll_state(self):
        # Mostrar mensajes
        while self.state.mensajes:
            lvl, msg = self.state.mensajes.pop(0)
            if lvl == "info":
                dialogs.Messagebox.show_info(msg)
            else:
                dialogs.Messagebox.show_error(msg)

        # Actualizar vista con filtros
        filtered = self._filter_orders(self.state.visibles)
        if self.orders_need_update(filtered):
            self._populate_orders(filtered)
        self.after(1000, self.poll_state)

    def orders_need_update(self, orders: List[Any]) -> bool:
        new_ids = [str(o.pack_id or o.id) for o in orders]
        return new_ids != self._current_ids

    def _populate_orders(self, orders: List[Any]):
        """Llena el canvas con los pedidos como frames individuales (estilo GUI 1)."""
        # Limpiar frames anteriores
        for frame in self.order_frames:
            frame.destroy()
        self.order_frames.clear()
        self._current_ids = [str(o.pack_id or o.id) for o in orders]
        
        # Detectar packs múltiples (mismo pack_id) solo en los pedidos filtrados
        pack_items = {}
        pack_order_count = {}  # Contar cuántos pedidos por pack_id
        pack_orders = {}  # Almacenar las órdenes por pack_id

        # Analizar solo los pedidos filtrados para detectar packs múltiples
        for ord_obj in orders:
            pack_id = str(ord_obj.pack_id or ord_obj.id)  # Asegurar que sea string
            if pack_id not in pack_items:
                pack_items[pack_id] = []
                pack_order_count[pack_id] = 0
                pack_orders[pack_id] = []
            
            pack_order_count[pack_id] += 1
            pack_orders[pack_id].append(ord_obj)
            for item in ord_obj.items:
                pack_items[pack_id].append(item)
        
        # Debug: Mostrar información de packs detectados
        multi_packs = {pid: count for pid, count in pack_order_count.items() if count > 1}
        if multi_packs:
            print(f"🔗 PACKS MÚLTIPLES DETECTADOS: {multi_packs}")
        else:
            print("⚠️ No se detectaron packs múltiples en esta actualización")
        
        # Crear frame para cada pedido (solo los filtrados)
        for ord_obj in orders:
            self._render_order(ord_obj, pack_items, pack_order_count)
    
    def _render_order(self, ord_obj, pack_items, pack_order_count):
        """Renderiza un pedido individual como frame con estilo GUI 1."""
        pack_id = str(ord_obj.pack_id or ord_obj.id)  # Asegurar que sea string
        items_in_pack = pack_items[pack_id]
        orders_in_pack = pack_order_count[pack_id]
        
        # Determinar si es un pack múltiple (múltiples pedidos o múltiples artículos)
        is_multi_pack = orders_in_pack > 1 or len(items_in_pack) > 1
        
        # Debug: Mostrar información del pack actual
        if is_multi_pack:
            print(f"🔗 RENDERIZANDO PACK MÚTIPLE: {pack_id} - {orders_in_pack} órdenes, {len(items_in_pack)} items")
        else:
            print(f"📝 Renderizando pedido individual: {pack_id}")
        
        # Determinar color de borde y estado según shipping_substatus
        status_raw = (ord_obj.shipping_substatus or '').lower()
        
        if is_multi_pack:
            # PACK MÚLTIPLE: Color violeta prominente
            border_color = '#9c27b0'  # violeta brillante
            pack_bg_color = '#4a148c'  # violeta oscuro para el fondo del frame
            if status_raw == 'printed':
                status_bg_color = '#6a1b9a'  # violeta medio para estado
                status_text = f'🔗 PACK IMPRESO ({orders_in_pack} pedidos)'
            elif status_raw == 'ready_to_print':
                status_bg_color = '#8e24aa'  # violeta medio para estado
                status_text = f'🔗 PACK LISTO ({orders_in_pack} pedidos) 🖨️'
            else:
                status_bg_color = '#7b1fa2'  # violeta medio
                status_text = f'🔗 PACK ({orders_in_pack} pedidos)'
        else:
            # PEDIDO INDIVIDUAL: Colores estándar
            pack_bg_color = '#2b2b2b'  # fondo estándar
            if status_raw == 'printed':
                border_color = '#4caf50'  # verde brillante
                status_bg_color = '#2e7d32'  # verde oscuro
                status_text = 'IMPRESA ✓'
            elif status_raw == 'ready_to_print':
                border_color = '#f44336'  # rojo brillante
                status_bg_color = '#c62828'  # rojo oscuro
                status_text = 'LISTA PARA IMPRIMIR 🖨️'
            else:
                border_color = '#9e9e9e'  # gris
                status_bg_color = '#424242'  # gris oscuro
                status_text = status_raw.upper() if status_raw else 'SIN ESTADO'
        
        # Frame principal del pedido con bordes coloreados
        pedido_fr = tk.Frame(
            self.inner_frame,
            padx=8,
            pady=8,
            relief="solid",
            borderwidth=4 if is_multi_pack else 2,  # Borde más grueso para packs
            bg=pack_bg_color,  # fondo según tipo de pack
            highlightthickness=4 if is_multi_pack else 3,  # Resaltado más grueso para packs
            highlightbackground=border_color,  # borde coloreado según estado
            highlightcolor=border_color
        )
        pedido_fr.pack(fill="x", pady=6, padx=8)
        self.order_frames.append(pedido_fr)
        
        # Cabecera del pedido
        head = tk.Frame(pedido_fr, bg=pack_bg_color)
        head.pack(fill="x", pady=(0, 4))
        
        # Comprador
        buyer_color = "#e1bee7" if is_multi_pack else "#ffcc80"  # Violeta claro para packs
        tk.Label(head, text=(ord_obj.buyer or "Sin comprador").upper(), 
                font=("Arial", 12, "bold"), bg=pack_bg_color, fg=buyer_color).pack(side=LEFT, padx=(0,12))
        
        # Pack ID con indicador de pack múltiple
        pack_text = f"🔗 #{pack_id}" if is_multi_pack else f"#{pack_id}"
        pack_color = "#ce93d8" if is_multi_pack else "#4dabf7"  # Violeta medio para packs
        tk.Label(head, text=pack_text, 
                font=("Arial", 14, "bold"), bg=pack_bg_color, fg=pack_color).pack(side=LEFT, padx=(4, 8))
        
        # Estado con colores más prominentes
        tk.Label(head, text=status_text, font=("Arial", 11, "bold"), 
                bg=status_bg_color, fg="white", padx=12, pady=4, 
                relief="raised", borderwidth=2).pack(side=RIGHT)
        
        # Renderizar cada ítem del pedido
        for item in ord_obj.items:
            self._render_item(pedido_fr, item, items_in_pack, ord_obj, is_multi_pack, pack_bg_color)
    
    def _render_item(self, parent, item, items_in_pack, ord_obj, is_multi_pack, pack_bg_color):
        """Renderiza un ítem individual dentro del pedido."""
        # Determinar símbolo según tipo de pack
        multiventa_symbol = ""
        
        if is_multi_pack:
            if len(items_in_pack) > 1:
                multiventa_symbol = " 🔗"  # Pack con múltiples artículos
            elif (item.quantity or 0) > 1:
                multiventa_symbol = " 🔢"  # Artículo con cantidad > 1
        
        # Color según tipo de pack (heredar del frame principal)
        if is_multi_pack:
            # Pack múltiple: usar colores violeta más suaves para items
            item_bg = '#6a1b9a'  # violeta medio (más claro que el frame principal)
            item_fg = '#e1bee7'  # violeta claro
            item_relief = "solid"
            item_borderwidth = 1
            item_highlightthickness = 1
            item_highlightbackground = '#ce93d8'  # violeta suave
        else:
            # Pedido individual: colores estándar
            item_bg = pack_bg_color  # Heredar fondo del frame principal
            item_fg = '#ffffff'
            item_relief = "flat"
            item_borderwidth = 0
            item_highlightthickness = 0
            item_highlightbackground = pack_bg_color
        
        # Frame del ítem con bordes según tipo
        item_fr = tk.Frame(parent, bg=item_bg, padx=4, pady=2,
                          relief=item_relief, borderwidth=item_borderwidth,
                          highlightthickness=item_highlightthickness,
                          highlightbackground=item_highlightbackground)
        item_fr.pack(fill="x", pady=1)
        
        # Artículo
        art_text = (item.title or "")[:60] + multiventa_symbol
        tk.Label(item_fr, text=art_text, font=("Arial", 11, "bold"), 
                bg=item_bg, fg=item_fg, anchor="w").pack(side=LEFT, fill="x", expand=True)
        
        # Talle
        talle = getattr(item, "size", "") or ""
        if talle:
            tk.Label(item_fr, text=f"T:{talle}", font=("Arial", 10), 
                    bg=item_bg, fg=item_fg, width=8).pack(side=RIGHT, padx=2)
        
        # Color
        color = getattr(item, "color", "") or ""
        if color:
            tk.Label(item_fr, text=f"C:{color}", font=("Arial", 10), 
                    bg=item_bg, fg=item_fg, width=12).pack(side=RIGHT, padx=2)
        
        # Cantidad
        cant = int(item.quantity or 0)
        tk.Label(item_fr, text=f"Cant:{cant}", font=("Arial", 11, "bold"), 
                bg=item_bg, fg="#ffeb3b", width=8).pack(side=RIGHT, padx=4)
        
        # Botón Pickear si es multiventa
        if is_multi_pack:
            btn_pick = tk.Button(item_fr, text="📦", font=("Arial", 8), 
                               bg="#ff9800", fg="white", bd=1, padx=4, pady=1,
                               command=lambda: self._show_multiventa_modal(ord_obj, item))
            btn_pick.pack(side=RIGHT, padx=2)

    def _row_dict(self, ord_obj, item) -> Dict[str, Any]:
        return {
            "order_id": ord_obj.id,
            "pack_id": ord_obj.pack_id,
            "shipping_id": ord_obj.shipping_id,
            "buyer": ord_obj.buyer,
            "notes": ord_obj.notes,
            "status": ord_obj.shipping_status,
            "substatus": ord_obj.shipping_substatus,
            "sku": item.sku,
            "barcode": item.barcode,
            "art": item.title,
            "talle": getattr(item, "size", "") or getattr(item, "talle", ""),
            "color": getattr(item, "color", ""),
            "cant": item.quantity,
        }

    # ------------------------------------------------------------------
    # Carga de pedidos
    # ------------------------------------------------------------------
    def load_orders(self):
        try:
            df = datetime.strptime(self.var_from.get(), "%d/%m/%Y")
            dt = datetime.strptime(self.var_to.get(), "%d/%m/%Y")
        except ValueError:
            dialogs.Messagebox.show_error("Formato de fecha inválido")
            return
        self.configure(cursor="watch")
        self.update_idletasks()

        import threading, traceback
        def worker():
            try:
                orders = self.picker.load_orders(df, dt)
                self.state.visibles = orders
                if not self._refresher_started:
                    self.refresher.start()
                    self._refresher_started = True
                self.after(0, lambda: self._populate_orders(self._filter_orders(orders)))
                self.after(0, lambda: self.btn_pick.configure(state=NORMAL))
            except Exception as e:
                self.after(0, lambda: dialogs.Messagebox.show_error(str(e)))
                traceback.print_exc()
            finally:
                self.after(0, lambda: self.configure(cursor=""))
        threading.Thread(target=worker, daemon=True).start()

    # ------------------------------------------------------------------
    # Acción botones
    # ------------------------------------------------------------------
    def open_pick_window(self):
        """Ventana simplificada para escanear códigos y registrar pickeo."""
        if not self.state.visibles:
            dialogs.Messagebox.show_info("Primero cargue pedidos")
            return
        self.picker.start_pick_session(self.state.visibles)
        win = tk.Toplevel(self)
        win.title("Pickeo Libre")
        win.transient(self)
        tb.Label(win, text="Escanee código de barras:").pack(pady=8)
        ent = tb.Entry(win, font=("Arial", 14))
        ent.pack(padx=20, pady=8)
        ent.focus()
        lbl = tb.Label(win, text="", font=("Arial", 12, "bold"))
        lbl.pack(pady=4)

        def procesar(event=None):
            code = ent.get().strip()
            if not code:
                return
            ok, msg = self.picker.scan_barcode(code)
            lbl.configure(text=msg, bootstyle=SUCCESS if ok else DANGER)
            ent.delete(0, tk.END)
            if ok:
                # Verificar si es multiventa y mostrar ventana correspondiente
                multiventa_info = self._check_multiventa_after_pick(code)
                if multiventa_info:
                    self._show_multiventa_window(multiventa_info, win)
                
                # refrescar tabla inmediatamente
                self._populate_orders(self._filter_orders(self.state.visibles))
                # agregar a historial
                for o in self.state.visibles:
                    for it2 in o.items:
                        if it2.barcode == code or it2.sku == code:
                            self.state.procesados.append(self._row_dict(o, it2))
                            break
        ent.bind('<Return>', procesar)

    def on_tree_select(self, event):
        pass  # No implementado en esta versión

    def on_info(self):
        # Función deshabilitada temporalmente - requiere adaptación al nuevo sistema
        dialogs.Messagebox.show_info("Función INFO temporalmente deshabilitada")
        return

    def on_fallado(self):
        # Función deshabilitada temporalmente - requiere adaptación al nuevo sistema  
        dialogs.Messagebox.show_info("Función FALLADO temporalmente deshabilitada")
        return

    def show_info(self, data: Dict[str, Any]):
        win = tk.Toplevel(self)
        win.title("Detalle del artículo")
        win.transient(self)
        frm = tb.Frame(win, padding=10)
        frm.pack(fill=BOTH, expand=YES)
        for idx, (k, v) in enumerate(data.items()):
            tb.Label(frm, text=f"{k}", font=("Arial", 9, "bold"), anchor=W).grid(row=idx, column=0, sticky=W, padx=4, pady=2)
            tb.Label(frm, text=str(v)).grid(row=idx, column=1, sticky=W, padx=4, pady=2)

    def open_fallado_modal(self, data: Dict[str, Any]):
        win = tk.Toplevel(self)
        win.title("Marcar FALLADO")
        win.transient(self)
        win.grab_set()
        frm = tb.Frame(win, padding=10)
        frm.pack(fill=BOTH, expand=YES)

        tb.Label(frm, text=f"Artículo: {data['art']}").pack(anchor=W)
        tb.Label(frm, text=f"Talle: {data['talle']}  Color: {data['color']}  Cant: {data['cant']}").pack(anchor=W)

        motivo_var = tk.StringVar(value=DEFAULT_MOTIVOS[0])
        for m in DEFAULT_MOTIVOS:
            tb.Radiobutton(frm, text=m, variable=motivo_var, value=m).pack(anchor=W)

        def aceptar():
            try:
                fail_service.cancel_item(data["pack_id"] or data["order_id"], data["sku"], motivo_var.get())
                # mover a procesados y quitar de visibles
                self.state.procesados.append(data)
                # quitar ítem de visibles
                for o in self.state.visibles:
                    if (o.pack_id or o.id) == data["pack_id"] or o.id == data["order_id"]:
                        try:
                            o.items = [it for it in o.items if it.sku != data["sku"]]
                        except Exception:
                            pass
                dialogs.Messagebox.show_info("Artículo cancelado y stock repuesto")
            except Exception as exc:
                dialogs.Messagebox.show_error(str(exc))
            win.destroy()

        tb.Button(frm, text="Aceptar", bootstyle=SUCCESS, command=aceptar).pack(pady=8)

    def on_print_list(self):
        # --- Filtrar SOLO por DEPO/DEPOSITO (no otros depósitos) ---
        # Solo procesar pedidos de este depósito específico
        rows = []
        
        # Contar pedidos procesados para debug
        total_visibles = len(self.state.visibles)
        pedidos_con_deposito = 0
        pedidos_ready_to_print = 0
        
        for ord_obj in self.state.visibles:
            nota = (ord_obj.notes or "").upper()
            
            # SEGURIDAD: Solo procesar pedidos que contengan DEPO o DEPOSITO
            # NO procesar pedidos de otros depósitos (CABA, MUNDOAL, etc.)
            if not ('DEPO' in nota or 'DEPOSITO' in nota):
                continue
            
            pedidos_con_deposito += 1
            
            # Filtrar por estado ready_to_print
            sub = (ord_obj.shipping_substatus or '').lower()
            if sub != 'ready_to_print':
                continue
                
            pedidos_ready_to_print += 1
            
            # Determinar qué depósito es (solo DEPO/DEPOSITO permitidos)
            # El depósito se extrae de la nota del pedido (ord_obj.notes)
            deposito_asignado = "N/A"
            if 'DEPOSITO' in nota:
                deposito_asignado = "DEPOSITO"
            elif 'DEPO' in nota:
                deposito_asignado = "DEPO"
            else:
                # Esto no debería pasar debido al filtro anterior, pero por seguridad
                continue
            
            # Agregar todos los items de este pedido
            for it in ord_obj.items:
                rows.append({
                    "art": it.title or "",
                    "talle": getattr(it, "size", "") or "",
                    "color": getattr(it, "color", "") or "",
                    "cant": int(it.quantity or 0),
                    "deposito": deposito_asignado,  # Nueva columna con el depósito asignado
                })
        
        # Mostrar información de debug
        print(f"DEBUG - Total pedidos visibles: {total_visibles}")
        print(f"DEBUG - Pedidos con depósito: {pedidos_con_deposito}")
        print(f"DEBUG - Pedidos ready_to_print: {pedidos_ready_to_print}")
        print(f"DEBUG - Total filas para imprimir: {len(rows)}")
        
        if not rows:
            dialogs.Messagebox.show_info(f"No hay filas para imprimir.\n\nDe {total_visibles} pedidos visibles:\n- {pedidos_con_deposito} contienen depósitos válidos\n- {pedidos_ready_to_print} están en estado ready_to_print")
            return
            
        try:
            # Generar PDF y abrirlo en el explorador para impresión manual
            print_service.generate_and_open_pdf(rows)
        except Exception as e:
            dialogs.Messagebox.show_error(f"Error al generar PDF: {str(e)}")
            return
        dialogs.Messagebox.show_info(f"PDF generado y abierto\n\n{len(rows)} artículos de {pedidos_ready_to_print} pedidos con depósito")

    # ------------------------------------------------------------------
    # Más opciones
    # ------------------------------------------------------------------
    def choose_printer(self):
        printers = [p[2] for p in win32print.EnumPrinters(win32print.PRINTER_ENUM_LOCAL | win32print.PRINTER_ENUM_CONNECTIONS)]
        if not printers:
            dialogs.Messagebox.show_error("No se encontraron impresoras")
            return
        dlg = tk.Toplevel(self)
        dlg.title("Seleccione impresora")
        lb = tk.Listbox(dlg, height=10, width=60)
        for pr in printers:
            lb.insert(tk.END, pr)
        lb.pack(padx=10, pady=10)

        def select():
            sel = lb.curselection()
            if sel:
                config.PRINTER_NAME = lb.get(sel[0])
                dialogs.Messagebox.show_info(f"Impresora seleccionada: {config.PRINTER_NAME}")
                dlg.destroy()
        tk.Button(dlg, text="Seleccionar", command=select).pack(pady=5)

    def test_printer(self):
        sample_zpl = b"^XA^CF0,50^FO50,50^FDTEST OK^FS^XZ"
        try:
            if not config.PRINTER_NAME:
                raise ValueError("Seleccione impresora primero")
            print_service.print_zpl_raw(sample_zpl, config.PRINTER_NAME)
            dialogs.Messagebox.show_info("Etiqueta de prueba enviada")
        except Exception as exc:
            dialogs.Messagebox.show_error(str(exc))

    # ------------------------------------------------------------------
    # Historial
    # ------------------------------------------------------------------
    def open_historial(self):
        win = tk.Toplevel(self)
        win.title("Historial de procesados")
        win.geometry("800x400")
        tree = ttk.Treeview(win, columns=("art", "talle", "color", "cant", "motivo"), show="headings")
        for col in ("art", "talle", "color", "cant", "motivo"):
            tree.heading(col, text=col.capitalize())
            tree.column(col, anchor="center", width=120)
        tree.pack(fill=BOTH, expand=YES)

        # Rellenar
        for idx, d in enumerate(self.state.procesados):
            tree.insert("", "end", iid=str(idx), values=(d["art"], d["talle"], d["color"], d["cant"], d.get("motivo", "")))

        # Reimprimir
        def reprint():
            sel = tree.selection()
            if not sel:
                return
            d = self.state.procesados[int(sel[0])]
            try:
                # reimprimir etiqueta; descarga de nuevo si es necesario
                if d.get("shipping_id"):
                    self.picker.print_shipping_label(d["shipping_id"])
                    dialogs.Messagebox.show_info("Etiqueta reimpresa")
            except Exception as exc:
                dialogs.Messagebox.show_error(str(exc))
        tb.Button(win, text="Reimprimir etiqueta", command=reprint).pack(pady=8)

    # ------------------------------------------------------------------
    # Funciones auxiliares para multiventa
    # ------------------------------------------------------------------
    def _check_multiventa_after_pick(self, barcode: str) -> dict | None:
        """
        Verifica si el artículo recién pickeado pertenece a un pack multiventa
        y retorna información sobre los artículos restantes del pack.
        """
        # Buscar el artículo que se acaba de pickear
        picked_order = None
        picked_pack_id = None
        
        for ord_obj in self.state.visibles:
            for item in ord_obj.items:
                if item.barcode == barcode or item.sku == barcode:
                    picked_order = ord_obj
                    picked_pack_id = str(ord_obj.pack_id or ord_obj.id)
                    break
            if picked_order:
                break
        
        if not picked_order or len(picked_order.items) <= 1:
            return None  # No es multiventa
        
        # Obtener artículos agrupados por nombre del mismo pack
        from collections import defaultdict
        
        # Diccionarios para agrupar por nombre del artículo
        pending_by_name = defaultdict(lambda: {'count': 0, 'details': []})
        picked_by_name = defaultdict(lambda: {'count': 0, 'details': []})
        
        # Procesar artículos pendientes
        for tup in self.picker._pending_units:
            ord_obj, idx_item, unit_idx = tup
            pack_id = str(ord_obj.pack_id or ord_obj.id)
            if pack_id == picked_pack_id:
                item = ord_obj.items[idx_item]
                item_name = item.title
                
                # Agregar detalles de talle/color si existen
                talle = getattr(item, 'size', '') or getattr(item, 'talle', '')
                color = getattr(item, 'color', '')
                detail_str = ""
                if talle and color:
                    detail_str = f" (Talle: {talle}, Color: {color})"
                elif talle:
                    detail_str = f" (Talle: {talle})"
                elif color:
                    detail_str = f" (Color: {color})"
                
                full_name = item_name + detail_str
                pending_by_name[full_name]['count'] += 1
                pending_by_name[full_name]['details'].append({
                    'sku': item.sku,
                    'barcode': item.barcode
                })
        
        # Procesar artículos pickeados
        for tup in self.picker._picked_units:
            ord_obj, idx_item, unit_idx = tup
            pack_id = str(ord_obj.pack_id or ord_obj.id)
            if pack_id == picked_pack_id:
                item = ord_obj.items[idx_item]
                item_name = item.title
                
                # Agregar detalles de talle/color si existen
                talle = getattr(item, 'size', '') or getattr(item, 'talle', '')
                color = getattr(item, 'color', '')
                detail_str = ""
                if talle and color:
                    detail_str = f" (Talle: {talle}, Color: {color})"
                elif talle:
                    detail_str = f" (Talle: {talle})"
                elif color:
                    detail_str = f" (Color: {color})"
                
                full_name = item_name + detail_str
                picked_by_name[full_name]['count'] += 1
                picked_by_name[full_name]['details'].append({
                    'sku': item.sku,
                    'barcode': item.barcode
                })
        
        if pending_by_name:  # Solo mostrar ventana si quedan artículos pendientes
            return {
                'pack_id': picked_pack_id,
                'total_items': len(picked_order.items),
                'pending_by_name': pending_by_name,
                'picked_by_name': picked_by_name,
                'buyer': picked_order.buyer
            }
        
        return None
    
    def _show_multiventa_window(self, multiventa_info: dict, parent_window):
        """
        Muestra una ventana con información de multiventa indicando
        los artículos restantes por pickear del pack agrupados por nombre.
        """
        pack_id = multiventa_info['pack_id']
        pending_by_name = multiventa_info['pending_by_name']
        picked_by_name = multiventa_info['picked_by_name']
        total_items = multiventa_info['total_items']
        buyer = multiventa_info['buyer']
        
        # Crear ventana modal
        win = tk.Toplevel(parent_window)
        win.title(f"MULTIVENTA - Pack {pack_id}")
        win.transient(parent_window)
        win.grab_set()
        win.geometry("600x400")
        
        # Frame principal
        main_frame = tb.Frame(win, padding=15)
        main_frame.pack(fill=BOTH, expand=YES)
        
        # Título
        title_label = tb.Label(
            main_frame, 
            text=f"🔗 MULTIVENTA DETECTADA", 
            font=("Arial", 16, "bold"),
            bootstyle=WARNING
        )
        title_label.pack(pady=(0, 10))
        
        # Información del pack
        info_frame = tb.Frame(main_frame)
        info_frame.pack(fill=X, pady=(0, 15))
        
        tb.Label(info_frame, text=f"Pack ID: {pack_id}", font=("Arial", 12, "bold")).pack(anchor=W)
        tb.Label(info_frame, text=f"Comprador: {buyer}", font=("Arial", 10)).pack(anchor=W)
        tb.Label(info_frame, text=f"Total artículos: {total_items}", font=("Arial", 10)).pack(anchor=W)
        
        # Artículos pickeados
        if picked_by_name:
            total_picked = sum(info['count'] for info in picked_by_name.values())
            picked_label = tb.Label(
                main_frame, 
                text=f"✅ Artículos ya pickeados ({total_picked} unidades):", 
                font=("Arial", 12, "bold"),
                bootstyle=SUCCESS
            )
            picked_label.pack(anchor=W, pady=(10, 5))
            
            for item_name, info in picked_by_name.items():
                item_text = f"• {item_name} × {info['count']}"
                tb.Label(main_frame, text=item_text, font=("Arial", 10), bootstyle=SUCCESS).pack(anchor=W, padx=20)
        
        # Artículos pendientes
        if pending_by_name:
            total_pending = sum(info['count'] for info in pending_by_name.values())
            pending_label = tb.Label(
                main_frame, 
                text=f"⏳ Artículos por pickear ({total_pending} unidades):", 
                font=("Arial", 12, "bold"),
                bootstyle=DANGER
            )
            pending_label.pack(anchor=W, pady=(15, 5))
            
            # Crear frame con scroll para la lista de pendientes
            pending_frame = tb.Frame(main_frame)
            pending_frame.pack(fill=BOTH, expand=YES, padx=20)
            
            for item_name, info in pending_by_name.items():
                # Crear frame para cada fila de artículo
                item_frame = tb.Frame(pending_frame, bootstyle=DANGER)
                item_frame.pack(fill=X, pady=2)
                
                item_text = f"• {item_name} × {info['count']}"
                tb.Label(item_frame, text=item_text, font=("Arial", 11, "bold"), bootstyle=DANGER).pack(anchor=W)
        
        # Mensaje de instrucción
        instruction_frame = tb.Frame(main_frame, bootstyle=INFO)
        instruction_frame.pack(fill=X, pady=20)
        
        instruction_text = (
            "⚠️ IMPORTANTE: Este pack tiene múltiples artículos.\n"
            "Debe pickear TODOS los artículos antes de que se imprima la etiqueta.\n"
            "Continúe escaneando los códigos restantes."
        )
        
        tb.Label(
            instruction_frame, 
            text=instruction_text, 
            font=("Arial", 10),
            justify=CENTER,
            bootstyle=INFO
        ).pack(pady=10)
        
        # Botón cerrar
        tb.Button(
            main_frame, 
            text="Entendido - Continuar", 
            command=win.destroy,
            bootstyle=PRIMARY
        ).pack(pady=10)
        
        # Centrar ventana
        win.update_idletasks()
        x = (win.winfo_screenwidth() // 2) - (win.winfo_width() // 2)
        y = (win.winfo_screenheight() // 2) - (win.winfo_height() // 2)
        win.geometry(f"+{x}+{y}")


# ----------------------------------------------------------------------
# Launch helper
# ----------------------------------------------------------------------

def launch_gui_v2():
    app = AppV2()
    app.mainloop()


if __name__ == "__main__":
    launch_gui_v2()
