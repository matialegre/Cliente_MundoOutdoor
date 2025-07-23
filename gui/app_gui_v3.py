"""Fase 3 – GUI simplificada mejorada con tabla usando ttkbootstrap.

Este módulo crea `AppV3`, una ventana optimizada que muestra los pedidos
en una tabla con columnas: Nombre completo, Cantidad, Talle, Color.
Mantiene toda la lógica de backend implementada sin modificar servicios.
Mejoras: colores amigables, botón Pickear grande, PDF con nombres completos.
"""
from __future__ import annotations

# Permitir ejecución directa añadiendo el directorio raíz al sys.path
import sys, pathlib
ROOT_DIR = pathlib.Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from datetime import datetime, timezone, timedelta
import tkinter as tk
from tkinter import ttk
import ttkbootstrap as tb
from ttkbootstrap.constants import *
import ttkbootstrap.dialogs as dialogs
import win32print  # type: ignore

from typing import List, Dict, Any

from services.picker_service import PickerService
from services import print_service, fail_service
from services.cancellation_service import cancellation_service
from models.gui_state import GuiState
from services.refresh_service import OrderRefresher
from printing.zpl_printer import print_zpl
from utils import config
from utils.daily_stats import get_packages_today, get_picked_today
from utils.daily_cache import daily_cache
from utils.logger import get_logger

log = get_logger(__name__)

__all__ = ["launch_gui_v3", "AppV3"]

DEFAULT_MOTIVOS = ["Manchado", "Roto", "No está"]
KEYWORDS_NOTE = ['DEPO', 'MUNDOAL', 'MTGBBL', 'BBPS', 'MONBAHIA', 'MTGBBPS']


class AppV3(tb.Window):
    # Columnas optimizadas: Nombre completo, Cantidad, Talle, Color
    COLS = ("nombre", "cant", "talle", "color")

    def __init__(self) -> None:
        super().__init__(title="Cliente Matías – Tabla Optimizada", themename="darkly")
        self.geometry("1200x700")

        # --- servicios / estado ---
        self.picker = PickerService()
        self.state = GuiState()
        self.refresher = OrderRefresher(self.picker, self.state, notification_callback=self._show_new_sale_notification)
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
        # Checkboxes con callbacks reactivos para actualizar tabla inmediatamente
        cb_printed = tb.Checkbutton(frm_top, text="Incluir impresos", variable=self.var_include_printed, command=self.on_filter_change)
        cb_printed.grid(row=0, column=4, padx=6)
        
        cb_until13 = tb.Checkbutton(frm_top, text="Pedidos hasta 13 hs", variable=self.var_until_13, command=self.on_filter_change)
        cb_until13.grid(row=0, column=5, padx=6)

        btn_load = tb.Button(frm_top, text="Cargar", command=self.load_orders, bootstyle=PRIMARY)
        btn_load.grid(row=0, column=6, padx=6)

        btn_print = tb.Button(frm_top, text="Imprimir lista", command=self.on_print_list, bootstyle=INFO)
        btn_print.grid(row=0, column=7, padx=6)

        # --- Menú Más opciones ---
        more_menu = tk.Menu(self, tearoff=0)
        more_menu.add_command(label="Elegir impresora", command=self.choose_printer)
        more_menu.add_command(label="Test impresora", command=self.test_printer)
        more_menu.add_separator()
        more_menu.add_command(label="IMPRIMIR NQNSHOP", command=self.print_nqnshop)
        more_menu.add_command(label="IMPRIMIR MUNDOCABA", command=self.print_mundocaba)
        mb_more = tb.Menubutton(frm_top, text="Más opciones", menu=more_menu, bootstyle=SECONDARY)
        mb_more.grid(row=0, column=8, padx=6)

        # --- Historial ---
        mb_hist_menu = tk.Menu(self, tearoff=0)
        mb_hist_menu.add_command(label="Ver historial", command=self.open_historial)
        mb_hist_menu.add_separator()
        mb_hist_menu.add_command(label="📦 Paquetes de hoy", command=self.show_daily_stats)
        mb_hist_menu.add_command(label="📝 Artículos pickeados hoy", command=self.show_picked_items_today)
        mb_hist = tb.Menubutton(frm_top, text="Historial", menu=mb_hist_menu, bootstyle=SECONDARY)
        mb_hist.grid(row=0, column=9, padx=6)

        # Botones de acción principales arriba
        self.btn_info = tb.Button(frm_top, text="INFO", command=self.on_info, state=DISABLED)
        self.btn_info.grid(row=0, column=11, padx=4)
        self.btn_fallado = tb.Button(frm_top, text="FALLADO", bootstyle=DANGER, command=self.on_fallado, state=DISABLED)
        self.btn_fallado.grid(row=0, column=12, padx=4)

        # --- Treeview con imagen de fondo ---
        frm_tree = tb.Frame(self, padding=10)
        frm_tree.pack(fill=BOTH, expand=YES)
        
        # Configurar grid para el frame
        frm_tree.grid_rowconfigure(0, weight=1)
        frm_tree.grid_columnconfigure(0, weight=1)
        
        # Intentar cargar imagen de fondo
        self.bg_image = None
        self.bg_label = None
        try:
            # Buscar imagen de fondo en el directorio del proyecto
            import os
            
            # Obtener directorio base del proyecto
            script_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            
            possible_paths = [
                # Rutas relativas al directorio actual
                "fondo.png", "fondo.jpg", "background.png", "background.jpg",
                "logo.png", "logo.jpg", "assets/fondo.png", "assets/background.png",
                # Rutas absolutas al directorio del proyecto
                os.path.join(script_dir, "fondo.png"),
                os.path.join(script_dir, "fondo.jpg"),
                os.path.join(script_dir, "background.png"),
                os.path.join(script_dir, "background.jpg"),
                os.path.join(script_dir, "logo.png"),
                os.path.join(script_dir, "assets", "fondo.png")
            ]
            
            bg_path = None
            for path in possible_paths:
                if os.path.exists(path):
                    bg_path = path
                    break
            
            if bg_path:
                try:
                    # Intentar cargar imagen con PIL para mejor compatibilidad
                    from PIL import Image, ImageTk
                    
                    # Cargar y redimensionar imagen
                    pil_image = Image.open(bg_path)
                    # Redimensionar para que no sea demasiado pesada
                    pil_image = pil_image.resize((800, 600), Image.Resampling.LANCZOS)
                    # Hacer semi-transparente para que no interfiera con la tabla
                    pil_image = pil_image.convert("RGBA")
                    # Reducir opacidad al 30%
                    alpha = pil_image.split()[-1]
                    alpha = alpha.point(lambda p: int(p * 0.3))
                    pil_image.putalpha(alpha)
                    
                    self.bg_image = ImageTk.PhotoImage(pil_image)
                    # Crear label de fondo y enviarlo al fondo
                    self.bg_label = tk.Label(frm_tree, image=self.bg_image)
                    self.bg_label.place(x=0, y=0, relwidth=1, relheight=1)
                    # IMPORTANTE: Enviar al fondo para que el Treeview quede encima
                    self.bg_label.lower()
                    print(f"✅ Imagen de fondo cargada con PIL: {bg_path}")
                    
                except ImportError:
                    # Fallback a PhotoImage nativo (solo PNG/GIF pequeños)
                    try:
                        self.bg_image = tk.PhotoImage(file=bg_path)
                        self.bg_label = tk.Label(frm_tree, image=self.bg_image)
                        self.bg_label.place(x=0, y=0, relwidth=1, relheight=1)
                        # IMPORTANTE: Enviar al fondo para que el Treeview quede encima
                        self.bg_label.lower()
                        print(f"✅ Imagen de fondo cargada con PhotoImage: {bg_path}")
                    except tk.TclError as e:
                        print(f"⚠️ Error con PhotoImage (imagen muy grande?): {e}")
                        print("💡 Instala Pillow para mejor soporte: pip install Pillow")
                        
                except Exception as e:
                    print(f"⚠️ Error procesando imagen: {e}")
            else:
                print("ℹ️ No se encontró imagen de fondo.")
                print(f"📁 Directorio del proyecto: {script_dir}")
                print("📝 Coloca 'fondo.png' en el directorio raíz del proyecto.")
                
        except Exception as e:
            print(f"⚠️ Error cargando imagen de fondo: {e}")

        style = ttk.Style()
        from tkinter import font as tkfont
        tree_font = tkfont.Font(family="Arial", size=28)  # Más pequeño que antes
        line_height = tree_font.metrics("linespace")
        row_h = int(line_height * 1.8)  # Más compacto
        
        style.configure("Treeview", font=tree_font, rowheight=row_h)
        style.configure("Treeview.Heading", font=("Arial", 30, "bold"))

        # Colores amigables para estados
        style.map("Treeview", background=[("selected", "#1E88E5")])

        self.tree = ttk.Treeview(
            frm_tree,
            columns=self.COLS,
            show="headings",
            selectmode="browse",
            height=15,
        )
        
        # Configurar columnas optimizadas
        headings = ["Nombre del Artículo", "Cant", "Talle", "Color"]
        for col, hd in zip(self.COLS, headings):
            self.tree.heading(col, text=hd)
            if col == "nombre":
                self.tree.column(col, anchor="w", width=700)  # Más ancho para nombre completo
            elif col == "cant":
                self.tree.column(col, anchor="center", width=100)
            else:
                self.tree.column(col, anchor="center", width=150)
        
        vsb = ttk.Scrollbar(frm_tree, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscrollcommand=vsb.set)
        self.tree.grid(row=0, column=0, sticky=NSEW)
        
        # Colores inteligentes para filas según análisis de código de barra
        self.tree.tag_configure("printed", background="#4CAF50", foreground="white")      # 🟢 Verde para impresos
        self.tree.tag_configure("barcode_found", background="#FFC107", foreground="black") # 🟡 Amarillo: SKU encontrado en BD
        self.tree.tag_configure("barcode_custom", background="#FF9800", foreground="black") # 🟠 Naranja: Encontrado por seller_custom_field
        self.tree.tag_configure("barcode_missing", background="#F44336", foreground="white") # 🔴 Rojo: Sin código de barra
        self.tree.tag_configure("multipack", background="#8E24AA", foreground="white")    # 🟣 Violeta para packs multiventa
        
        vsb.grid(row=0, column=1, sticky=NS)
        frm_tree.rowconfigure(0, weight=1)
        frm_tree.columnconfigure(0, weight=1)

        # Marco inferior con Pickear GRANDE y CENTRADO
        action_frame = tb.Frame(self, padding=15)
        action_frame.pack(fill=X)
        
        # Configurar estilo personalizado para el botón Pickear
        style.configure("BigButton.TButton", font=("Arial", 16, "bold"))
        
        # Frame para organizar botón centrado y contador a la derecha
        button_container = tb.Frame(action_frame)
        button_container.pack(fill=X)
        
        # Botón Pickear más grande y centrado
        btn_pick = tb.Button(
            button_container, 
            text="🔍 PICKEAR", 
            bootstyle="success",  # Usar string en lugar de constante
            command=self.open_pick_window, 
            state=DISABLED,
            width=20,  # Más ancho
            style="BigButton.TButton"  # Aplicar estilo personalizado
        )
        btn_pick.pack(pady=10)  # Centrado con padding vertical
        self.btn_pick = btn_pick
        
        # Frame para barra de progreso y contador (abajo a la derecha)
        progress_frame = tb.Frame(action_frame)
        progress_frame.pack(side="right", anchor="se", padx=10, pady=5)
        
        # Barra de progreso del día
        self.progress_var = tk.DoubleVar(value=0)
        self.progress_bar = tb.Progressbar(
            progress_frame,
            variable=self.progress_var,
            maximum=100,
            length=200,
            bootstyle="success-striped",
            mode="determinate"
        )
        self.progress_bar.pack(pady=(0, 5))
        
        # Label de progreso con porcentaje
        self.lbl_progress = tb.Label(
            progress_frame,
            text="📊 Progreso del día: 0% (0/0)",
            font=("Arial", 10, "bold"),
            bootstyle="success"
        )
        self.lbl_progress.pack()
        
        # Contador de pedidos pendientes
        self.lbl_pending_count = tb.Label(
            progress_frame,
            text="📋 Pedidos pendientes: 0",
            font=("Arial", 12, "bold"),
            bootstyle="info"
        )
        self.lbl_pending_count.pack()

        self.tree.bind('<<TreeviewSelect>>', self.on_tree_select)
    
    def on_filter_change(self):
        """Callback reactivo cuando cambian los filtros (checkboxes)."""
        try:
            # Solo actualizar si ya hay datos cargados
            if hasattr(self.state, 'visibles') and self.state.visibles:
                # Aplicar filtros inmediatamente sin recargar desde ML
                filtered = self._filter_orders(self.state.visibles)
                
                # Actualizar tabla con nuevos filtros
                self.populate_tree(filtered)
                
                print(f"🔄 Filtros actualizados: Incluir impresos={self.var_include_printed.get()}, Hasta 13hs={self.var_until_13.get()}")
            else:
                print("ℹ️ No hay datos cargados. Presiona 'Cargar' primero.")
                
        except Exception as e:
            print(f"⚠️ Error aplicando filtros: {e}")

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
            # fecha con manejo correcto de zona horaria
            try:
                # MercadoLibre envía fechas en formato ISO, convertir a hora Argentina (UTC-3)
                if o.date_created.endswith('Z'):
                    # Formato UTC
                    ts_utc = datetime.fromisoformat(o.date_created.replace('Z', '+00:00'))
                else:
                    # Formato con zona horaria
                    ts_utc = datetime.fromisoformat(o.date_created)
                
                # Convertir a hora Argentina (UTC-3)
                argentina_tz = timezone(timedelta(hours=-3))
                ts_argentina = ts_utc.astimezone(argentina_tz)
                ts = ts_argentina  # Para compatibilidad con filtro de fechas
            except Exception as e:
                print(f"Error parseando fecha {o.date_created}: {e}")
                ts = None
                ts_argentina = None
            
            if ts and date_from and date_to and not (date_from <= ts.date() <= date_to):
                continue
            
            # Filtro "hasta las 13:00" usando hora Argentina - SOLO para el último día del rango
            if until_13 and ts_argentina and date_to and ts_argentina.date() == date_to:
                if ts_argentina.time() >= datetime.strptime("13:00", "%H:%M").time():
                    continue
            
            # Filtrar por palabras clave en nota - cualquiera de los depósitos válidos
            note_up = (o.notes or '').upper()
            
            # Verificar si contiene alguna de las palabras clave de depósitos
            has_keywords = any(keyword in note_up for keyword in KEYWORDS_NOTE)
            
            # Para packs multiventa, verificar también si ALGUNA orden del pack tiene keywords
            if not has_keywords and o.pack_id:
                # Buscar otras órdenes del mismo pack para verificar sus notas
                pack_orders = [ord_obj for ord_obj in orders if ord_obj.pack_id == o.pack_id]
                for pack_ord in pack_orders:
                    pack_note_up = (pack_ord.notes or '').upper()
                    if any(keyword in pack_note_up for keyword in KEYWORDS_NOTE):
                        has_keywords = True
                        break
            
            if not has_keywords:
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

        # Actualizar tabla con filtros
        filtered = self._filter_orders(self.state.visibles)
        
        # MEJORADO: NO actualizar si el usuario tiene una fila seleccionada o ventanas abiertas
        selected_items = self.tree.selection()
        has_open_dialogs = len(self.winfo_children()) > 3  # Detectar ventanas/diálogos abiertos
        
        if selected_items or has_open_dialogs:
            # Usuario interactuando - pausar actualizaciones completamente
            self.after(10000, self.poll_state)  # Revisar cada 10 segundos sin actualizar
        else:
            # Sin selección ni diálogos - actualizar solo si hay cambios
            if self.tree_needs_update(filtered):
                # Preservar scroll position si es posible
                try:
                    first_visible = self.tree.identify_row(0)
                    self.populate_tree(filtered)
                    if first_visible:
                        self.tree.see(first_visible)
                except:
                    self.populate_tree(filtered)
            # OPTIMIZADO: Sincronizar con backend thread (2 minutos)
            self.after(120000, self.poll_state)  # Actualizar cada 2 minutos

    def tree_needs_update(self, orders: List[Any]) -> bool:
        new_ids = [str(o.pack_id or o.id) for o in orders]
        return new_ids != self._current_ids
    
    def force_refresh_after_pick(self):
        """Fuerza refresco inmediato tras pickeo para actualizar estado."""
        try:
            # Recargar datos del backend sin esperar el ciclo
            filtered = self._filter_orders(self.state.visibles)
            
            # Actualizar tabla inmediatamente
            self.populate_tree(filtered)
            
            # Log para debug
            print("🔄 Refresco inmediato tras pickeo completado")
            
        except Exception as e:
            print(f"⚠️ Error en refresco inmediato: {e}")

    def populate_tree(self, orders):
        # Limpiar árbol
        for item in self.tree.get_children():
            self.tree.delete(item)
        
        # Solo analizar códigos de barra si los pedidos han cambiado
        current_order_ids = [str(o.pack_id or o.id) for o in orders]
        if not hasattr(self, '_last_analyzed_orders') or self._last_analyzed_orders != current_order_ids:
            log.info("🔍 Iniciando análisis de códigos de barra...")
            sku_analysis = self._analyze_barcode_status(orders)
            self._current_sku_analysis = sku_analysis
            self._last_analyzed_orders = current_order_ids
        else:
            log.debug("📋 Usando análisis de códigos de barra en caché")
        
        # Detectar packs multiventa para resaltado
        pack_counts = {}
        for ord_obj in orders:
            if ord_obj.pack_id:
                pack_counts[ord_obj.pack_id] = pack_counts.get(ord_obj.pack_id, 0) + 1
        
        multipack_ids = {pack_id for pack_id, count in pack_counts.items() if count > 1}
        
        for ord_obj in orders:
            # Detectar si es un pack multiventa (múltiples órdenes con mismo pack_id)
            is_multipack = ord_obj.pack_id in multipack_ids if ord_obj.pack_id else False
            
            for it in ord_obj.items:
                iid = f"{ord_obj.pack_id or ord_obj.id}|{it.sku}"
                # Nombre completo del artículo (sin truncar)
                nombre_completo = getattr(it, 'title', '') or 'Sin nombre'
                talle = getattr(it, "size", "") or getattr(it, "talle", "")
                color = getattr(it, "color", "")
                qty = getattr(it, "quantity", 1)
                
                # Determinar tag según estado y análisis de código de barra
                if (ord_obj.shipping_substatus or "").lower() == "printed":
                    tag = "printed"  # Verde para impresos
                elif is_multipack:
                    tag = "multipack"  # Violeta para packs multiventa
                    nombre_completo = f"🔗 {nombre_completo}"  # Símbolo de pack
                else:
                    # Aplicar colores según análisis de código de barra
                    barcode_status = self._current_sku_analysis.get(it.sku, {}).get('status', 'missing')
                    
                    if barcode_status == 'found':
                        tag = "barcode_found"  # Amarillo: SKU encontrado en BD
                    elif barcode_status == 'custom_field':
                        tag = "barcode_custom"  # Naranja: Encontrado por seller_custom_field
                    else:
                        tag = "barcode_missing"  # Rojo: Sin código de barra
                
                self.tree.insert("", "end", iid=iid, 
                               values=(nombre_completo, qty, talle, color), 
                               tags=(tag,))
        
        # Actualizar contador de pedidos pendientes y barra de progreso
        self._update_pending_counter()
        self._update_progress_bar()

    def _count_pending_orders(self) -> int:
        """Cuenta ARTÍCULOS pendientes (mismo criterio que barra de progreso)."""
        try:
            if not hasattr(self.state, 'visibles') or not self.state.visibles:
                return 0
            
            # Usar mismo filtro que la barra de progreso para consistencia
            filtered_orders = self._filter_orders(self.state.visibles)
            
            pending_articles = 0
            processed_packs = set()
            
            for order in filtered_orders:
                pack_key = order.pack_id or order.id
                
                # Evitar contar el mismo pack múltiples veces
                if pack_key in processed_packs:
                    continue
                processed_packs.add(pack_key)
                
                # Solo contar si está pendiente (ready_to_print)
                if (order.shipping_substatus or '').lower() == 'ready_to_print':
                    pending_articles += len(order.items)
            
            return pending_articles
            
        except Exception as e:
            print(f"Error contando artículos pendientes: {e}")
            return 0
    
    def _update_pending_counter(self):
        """Actualiza el contador de artículos pendientes."""
        pending_count = self._count_pending_orders()
        self.lbl_pending_count.config(text=f"📋 Artículos pendientes: {pending_count}")
    
    def _update_progress_bar(self):
        """Actualiza la barra de progreso con el % de pedidos completados del día."""
        try:
            if not hasattr(self.state, 'visibles') or not self.state.visibles:
                self.progress_var.set(0)
                self.lbl_progress.config(text="📊 Progreso del día: 0% (0/0)")
                return
            
            # Contar pedidos totales y completados del día (aplicando filtros)
            filtered_orders = self._filter_orders(self.state.visibles)
            
            total_orders = 0
            completed_orders = 0
            
            # Contar por pack_id único para evitar duplicados
            processed_packs = set()
            
            for order in filtered_orders:
                pack_key = order.pack_id or order.id
                
                # Evitar contar el mismo pack múltiples veces
                if pack_key in processed_packs:
                    continue
                processed_packs.add(pack_key)
                
                total_orders += 1
                
                # Considerar completado si está impreso (printed)
                if (order.shipping_substatus or '').lower() == 'printed':
                    completed_orders += 1
            
            # Calcular porcentaje
            if total_orders > 0:
                progress_percent = (completed_orders / total_orders) * 100
                self.progress_var.set(progress_percent)
                self.lbl_progress.config(
                    text=f"📊 Progreso del día: {progress_percent:.1f}% ({completed_orders}/{total_orders})"
                )
            else:
                self.progress_var.set(0)
                self.lbl_progress.config(text="📊 Progreso del día: 0% (0/0)")
                
        except Exception as e:
            print(f"⚠️ Error actualizando barra de progreso: {e}")
            self.progress_var.set(0)
            self.lbl_progress.config(text="📊 Progreso del día: Error")
    
    def _analyze_barcode_status(self, orders) -> Dict[str, Dict[str, Any]]:
        """Analiza el estado de códigos de barra para todos los SKUs visibles.
        
        Returns:
            Dict con formato: {sku: {'status': 'found'|'custom_field'|'missing', 'barcode': str|None, 'real_sku': str|None}}
        """
        sku_analysis = {}
        
        # Recopilar todos los SKUs únicos de los pedidos visibles
        all_skus = set()
        sku_to_item = {}  # Para acceder a item_id y variation_id
        
        for order in orders:
            for item in order.items:
                all_skus.add(item.sku)
                sku_to_item[item.sku] = {
                    'item_id': getattr(item, 'item_id', None),
                    'variation_id': getattr(item, 'variation_id', None),
                    'real_sku': getattr(item, 'real_sku', item.sku)
                }
        
        log.info(f"🔍 Analizando códigos de barra para {len(all_skus)} SKUs únicos")
        
        for sku in all_skus:
            try:
                # 1. Buscar directamente en base de datos por SKU
                barcode_from_db, _ = self.picker.get_ml_code_from_barcode_reverse(sku)
                
                if barcode_from_db:
                    # 🟡 AMARILLO: Encontrado en BD
                    sku_analysis[sku] = {
                        'status': 'found',
                        'barcode': barcode_from_db,
                        'real_sku': sku_to_item[sku]['real_sku']
                    }
                    log.debug(f"🟡 SKU {sku}: encontrado en BD con código {barcode_from_db}")
                else:
                    # 2. Buscar por seller_custom_field (real_sku)
                    real_sku = sku_to_item[sku]['real_sku']
                    if real_sku and real_sku != sku:
                        barcode_from_custom, _ = self.picker.get_ml_code_from_barcode_reverse(real_sku)
                        
                        if barcode_from_custom:
                            # 🟠 NARANJA: Encontrado por seller_custom_field
                            sku_analysis[sku] = {
                                'status': 'custom_field',
                                'barcode': barcode_from_custom,
                                'real_sku': real_sku
                            }
                            log.debug(f"🟠 SKU {sku}: encontrado por real_sku {real_sku} con código {barcode_from_custom}")
                        else:
                            # 🔴 ROJO: No encontrado
                            sku_analysis[sku] = {
                                'status': 'missing',
                                'barcode': None,
                                'real_sku': real_sku
                            }
                            log.debug(f"🔴 SKU {sku}: sin código de barra (real_sku: {real_sku})")
                    else:
                        # 🔴 ROJO: No encontrado y sin real_sku diferente
                        sku_analysis[sku] = {
                            'status': 'missing',
                            'barcode': None,
                            'real_sku': sku
                        }
                        log.debug(f"🔴 SKU {sku}: sin código de barra")
                        
            except Exception as e:
                log.warning(f"⚠️ Error analizando SKU {sku}: {e}")
                # En caso de error, marcar como missing
                sku_analysis[sku] = {
                    'status': 'missing',
                    'barcode': None,
                    'real_sku': sku_to_item[sku]['real_sku']
                }
        
        # Resumen de análisis
        found_count = sum(1 for s in sku_analysis.values() if s['status'] == 'found')
        custom_count = sum(1 for s in sku_analysis.values() if s['status'] == 'custom_field')
        missing_count = sum(1 for s in sku_analysis.values() if s['status'] == 'missing')
        
        log.info(f"📊 Análisis completado: 🟡{found_count} 🟠{custom_count} 🔴{missing_count}")
        
        return sku_analysis

    def _row_dict(self, ord_obj, item) -> Dict[str, Any]:
        real_sku = getattr(item, 'real_sku', None) or item.sku
        
        # Obtener información del análisis de código de barra
        barcode_info = "No analizado"
        barcode_from_analysis = None
        
        # Si tenemos análisis de códigos de barra guardado
        if hasattr(self, '_current_sku_analysis'):
            sku_data = self._current_sku_analysis.get(item.sku, {})
            status = sku_data.get('status', 'missing')
            barcode_from_analysis = sku_data.get('barcode')
            
            if status == 'found':
                barcode_info = f"🟡 Encontrado en BD: {barcode_from_analysis}"
            elif status == 'custom_field':
                barcode_info = f"🟠 Por seller_custom_field: {barcode_from_analysis}"
            else:
                barcode_info = "🔴 Sin código de barra asociado"
        
        return {
            "order_id": ord_obj.id,
            "pack_id": ord_obj.pack_id,
            "shipping_id": ord_obj.shipping_id,
            "buyer": ord_obj.buyer,
            "notes": ord_obj.notes,
            "status": ord_obj.shipping_status,
            "substatus": ord_obj.shipping_substatus,
            "sku": item.sku,  # SKU original de MercadoLibre
            "real_sku": real_sku,  # SKU real resuelto (seller_custom_field)
            "barcode": item.barcode or "None",  # Código original de ML
            "barcode_analysis": barcode_info,  # Análisis de código de barra
            "barcode_found": barcode_from_analysis or "N/A",  # Código encontrado en BD
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
                
                # Resolver real_sku para todos los items
                for order in orders:
                    for item in order.items:
                        try:
                            # Resolver SKU real usando SKUResolver
                            # Necesitamos item_id y variation_id del item
                            item_id = getattr(item, 'item_id', None)
                            variation_id = getattr(item, 'variation_id', None)
                            
                            if item_id:
                                real_sku = self.picker.sku_resolver.get_real_sku(item_id, variation_id, item.sku)
                                if real_sku and real_sku != item.sku:
                                    item.real_sku = real_sku
                                    log.debug(f"✅ SKU resuelto: {item.sku} → {real_sku}")
                                else:
                                    item.real_sku = item.sku
                            else:
                                # Si no hay item_id, usar SKU original
                                item.real_sku = item.sku
                        except Exception as e:
                            log.warning(f"⚠️ Error resolviendo SKU {item.sku}: {e}")
                            item.real_sku = item.sku
                
                self.state.visibles = orders
                if not self._refresher_started:
                    self.refresher.start()
                    self._refresher_started = True
                self.after(0, lambda: self.populate_tree(self._filter_orders(orders)))
                self.after(0, lambda: self.btn_pick.configure(state=NORMAL))
                self.after(0, lambda: self._update_pending_counter())
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
        
        # IMPORTANTE: Solo usar órdenes FILTRADAS (visibles en GUI), no todas las cargadas
        filtered_orders = self._filter_orders(self.state.visibles)
        if not filtered_orders:
            dialogs.Messagebox.show_info("No hay pedidos filtrados para pickear")
            return
            
        self.picker.start_pick_session(filtered_orders)
        win = tk.Toplevel(self)
        win.title("Pickeo Libre")
        win.transient(self)
        win.geometry("500x300")
        
        tb.Label(win, text="Escanee código de barras:", font=("Arial", 14)).pack(pady=15)
        ent = tb.Entry(win, font=("Arial", 16), width=30)
        ent.pack(padx=20, pady=10)
        ent.focus()
        
        lbl = tb.Label(win, text="", font=("Arial", 12, "bold"), wraplength=450)
        lbl.pack(pady=10, padx=20)

        def procesar(event=None):
            code = ent.get().strip()
            if not code:
                return
            ok, msg = self.picker.scan_barcode(code)
            lbl.configure(text=msg, bootstyle=SUCCESS if ok else DANGER)
            ent.delete(0, tk.END)
            if ok:
                # refrescar tabla inmediatamente
                self.populate_tree(self._filter_orders(self.state.visibles))
                
                # Verificar si es multiventa y mostrar ventana con artículos pendientes
                multiventa_info = self._check_multiventa_after_pick(code)
                if multiventa_info:
                    self._show_multiventa_window(multiventa_info, win)
                
                # agregar a historial
                for o in self.state.visibles:
                    for it2 in o.items:
                        if it2.barcode == code or it2.sku == code:
                            self.state.procesados.append(self._row_dict(o, it2))
                            break
        ent.bind('<Return>', procesar)

    def on_tree_select(self, event):
        sel = self.tree.selection()
        state = NORMAL if sel else DISABLED
        self.btn_info.configure(state=state)
        self.btn_fallado.configure(state=state)
        self.btn_pick.configure(state=NORMAL if self.state.visibles else DISABLED)

    def on_info(self):
        sel = self.tree.selection()
        if not sel:
            return
        iid = sel[0]
        ord_id, sku = iid.split('|', 1)
        data = None
        for o in self.state.visibles:
            if str(o.pack_id or o.id) == ord_id:
                for it in o.items:
                    if it.sku == sku:
                        data = self._row_dict(o, it)
                        break
        if data:
            self.show_info(data)

    def on_fallado(self):
        sel = self.tree.selection()
        if not sel:
            return
        iid = sel[0]
        ord_id, sku = iid.split('|', 1)
        data = None
        for o in self.state.visibles:
            if str(o.pack_id or o.id) == ord_id:
                for it in o.items:
                    if it.sku == sku:
                        data = self._row_dict(o, it)

        if data:
            self.open_fallado_modal(data)

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
                cancellation_service.cancel_item(data["pack_id"] or data["order_id"], data["sku"], motivo_var.get())
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
        # OPTIMIZADO: Usar solo órdenes ya filtradas en la GUI (no cargar todas)
        filtered_orders = self._filter_orders(self.state.visibles)
        
        if not filtered_orders:
            dialogs.Messagebox.show_info("No hay pedidos filtrados para imprimir")
            return
            
        rows = []
        
        for ord_obj in filtered_orders:
            # Determinar qué depósito es según las palabras clave encontradas
            nota = (ord_obj.notes or "").upper()
            deposito_asignado = "DEPOSITO"  # Default
            for keyword in KEYWORDS_NOTE:
                if keyword in nota:
                    deposito_asignado = keyword
                    break
            
            # Agregar todos los items de este pedido
            for it in ord_obj.items:
                rows.append({
                    "art": it.title or "Sin nombre",
                    "talle": getattr(it, "size", "") or getattr(it, "talle", ""),
                    "color": getattr(it, "color", ""),
                    "cant": int(it.quantity or 0),
                    "deposito": deposito_asignado
                })
        
        print(f"Items a imprimir: {len(rows)}")
        
        if not rows:
            dialogs.Messagebox.show_info("No hay artículos listos para imprimir")
            return
            
        if not config.PRINTER_NAME:
            dialogs.Messagebox.show_error("Seleccione impresora primero")
            return
            
        try:
            print_service.print_list_pdf(rows, config.PRINTER_NAME)
            dialogs.Messagebox.show_info(f"Lista enviada a impresora: {len(rows)} artículos")
        except TypeError:
            dialogs.Messagebox.show_error("Datos inválidos al generar PDF. Revise tamaños/cantidades.")
            return

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

    def print_nqnshop(self):
        """Genera PDF filtrado solo por pedidos que contengan NQNSHOP en las notas."""
        self._print_filtered_pdf("NQNSHOP", "Lista de artículos NQNSHOP")

    def print_mundocaba(self):
        """Genera PDF filtrado solo por pedidos que contengan MUNDOCAB, CABA o CAB en las notas."""
        self._print_filtered_pdf(["MUNDOCAB", "CABA", "CAB"], "Lista de artículos MUNDOCAB/CABA")

    def _print_filtered_pdf(self, filter_keywords, title: str):
        """Función auxiliar para generar PDFs filtrados por palabra(s) clave específica(s).
        
        Args:
            filter_keywords: Palabra clave (str) o lista de palabras clave a buscar en las notas
            title: Título del PDF
        """
        if not config.PRINTER_NAME:
            dialogs.Messagebox.show_error("Seleccione impresora primero")
            return

        # Convertir a lista si es string
        if isinstance(filter_keywords, str):
            filter_keywords = [filter_keywords]

        # CARGAR TODOS LOS PEDIDOS (no solo los visibles en la GUI)
        try:
            all_orders = self.picker.load_orders_cached()
            keywords_str = "/".join(filter_keywords)
            print(f"📋 Cargando todos los pedidos para filtrar por {keywords_str}: {len(all_orders)} pedidos")
        except Exception as e:
            dialogs.Messagebox.show_error(f"Error cargando pedidos: {str(e)}")
            return

        rows = []
        pedidos_encontrados = 0
        pedidos_ready_to_print = 0
        
        for ord_obj in all_orders:
            nota = (ord_obj.notes or "").upper()
            
            # Filtrar por cualquiera de las palabras clave
            if not any(keyword in nota for keyword in filter_keywords):
                continue
            
            pedidos_encontrados += 1
            
            # Filtrar por estado ready_to_print
            sub = (ord_obj.shipping_substatus or '').lower()
            if sub != 'ready_to_print':
                continue
                
            pedidos_ready_to_print += 1
            
            # Detectar qué keyword coincidió para el depósito
            matched_keyword = "DEPOSITO"  # Default
            for kw in filter_keywords:
                if kw in nota:
                    matched_keyword = kw
                    break
            
            # Agregar todos los items de este pedido
            for it in ord_obj.items:
                rows.append({
                    "art": it.title or "Sin nombre",
                    "talle": getattr(it, "size", "") or getattr(it, "talle", ""),
                    "color": getattr(it, "color", ""),
                    "cant": int(it.quantity or 0),
                    "deposito": matched_keyword
                })
        
        keywords_str = "/".join(filter_keywords)
        print(f"🔍 DEBUG IMPRESIÓN {keywords_str}:")
        print(f"  Total pedidos revisados: {len(all_orders)}")
        print(f"  Pedidos encontrados con {keywords_str}: {pedidos_encontrados}")
        print(f"  Pedidos ready_to_print con {keywords_str}: {pedidos_ready_to_print}")
        print(f"  Items a imprimir: {len(rows)}")
        
        if not rows:
            if pedidos_encontrados > 0:
                dialogs.Messagebox.show_info(f"Se encontraron {pedidos_encontrados} pedidos con {keywords_str}, pero ninguno está en estado 'ready_to_print'")
            else:
                dialogs.Messagebox.show_info(f"No se encontraron pedidos con {keywords_str} en las notas")
            return
        
        try:
            # Usar la función de impresión existente con título personalizado
            import tempfile
            import os
            from reportlab.lib.pagesizes import A4
            from reportlab.pdfgen import canvas
            
            # Generar PDF personalizado
            keywords_safe = "_".join(filter_keywords)
            fd, pdf_path = tempfile.mkstemp(suffix=f"_{keywords_safe}.pdf")
            os.close(fd)
            
            width, height = A4
            c = canvas.Canvas(pdf_path, pagesize=A4)
            
            # Cabecera personalizada
            y = height - 40
            c.setFont("Helvetica-Bold", 18)
            c.drawString(40, y, title)
            y -= 30
            
            # Encabezados de columnas
            c.setFont("Helvetica-Bold", 12)
            c.drawString(40,  y, "Artículo")
            c.drawString(280, y, "Talle")
            c.drawString(330, y, "Color")
            c.drawString(400, y, "Cant")
            c.drawString(440, y, "Depósito")
            c.drawString(515, y, "✔")
            y -= 8
            c.line(40, y, width - 40, y)
            y -= 22
            
            # Detalle
            c.setFont("Helvetica", 11)
            MAX_ART_LEN = 35
            
            def wrap_text(text, max_len):
                if not text or len(text) <= max_len:
                    return [text or ""]
                lines = []
                words = text.split()
                current_line = ""
                for word in words:
                    if len(current_line + " " + word) <= max_len:
                        current_line += (" " if current_line else "") + word
                    else:
                        if current_line:
                            lines.append(current_line)
                        current_line = word
                if current_line:
                    lines.append(current_line)
                return lines
            
            for r in rows:
                if y < 80:
                    c.showPage()
                    y = height - 60
                    c.setFont("Helvetica", 11)
                
                art = (r.get("art") or "")
                talle = (r.get("talle") or "")[:6]
                color = (r.get("color") or "")[:10]
                cant = str(r.get("cant") or "")
                deposito = (r.get("deposito") or "N/A")[:10]
                
                art_lines = wrap_text(art, MAX_ART_LEN)
                
                # Primera línea
                c.drawString(40, y, art_lines[0] if art_lines else "")
                c.drawString(280, y, talle)
                c.drawString(330, y, color)
                c.drawRightString(435, y, cant)
                c.drawString(440, y, deposito)
                c.rect(515, y - 4, 14, 14)
                
                # Líneas adicionales del artículo
                for additional_line in art_lines[1:]:
                    y -= 15
                    if y < 50:
                        c.showPage()
                        y = height - 60
                        c.setFont("Helvetica", 11)
                    c.drawString(40, y, additional_line)
                
                y -= 20
            
            c.save()
            
            # Abrir PDF automáticamente
            import win32api
            win32api.ShellExecute(0, "open", pdf_path, None, ".", 1)
            
            dialogs.Messagebox.show_info(f"PDF de {keywords_str} generado: {len(rows)} artículos")
            
        except Exception as exc:
            dialogs.Messagebox.show_error(f"Error generando PDF de {keywords_str}: {str(exc)}")

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
                    detail_str = f" (T:{talle}, C:{color})"
                elif talle:
                    detail_str = f" (T:{talle})"
                elif color:
                    detail_str = f" (C:{color})"
                
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
                    detail_str = f" (T:{talle}, C:{color})"
                elif talle:
                    detail_str = f" (T:{talle})"
                elif color:
                    detail_str = f" (C:{color})"
                
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
        Muestra "FALTAN X" y lista de artículos pendientes.
        """
        pack_id = multiventa_info['pack_id']
        pending_by_name = multiventa_info['pending_by_name']
        picked_by_name = multiventa_info['picked_by_name']
        total_items = multiventa_info['total_items']
        buyer = multiventa_info['buyer']
        
        # Calcular totales
        total_pending = sum(info['count'] for info in pending_by_name.values())
        total_picked = sum(info['count'] for info in picked_by_name.values())
        
        # Crear ventana modal
        win = tk.Toplevel(parent_window)
        win.title(f"MULTIVENTA - Pack {pack_id}")
        win.transient(parent_window)
        win.grab_set()
        win.geometry("700x500")
        
        # Frame principal
        main_frame = tb.Frame(win, padding=15)
        main_frame.pack(fill=BOTH, expand=YES)
        
        # Título con FALTAN X
        title_label = tb.Label(
            main_frame, 
            text=f"🔗 MULTIVENTA - FALTAN {total_pending}", 
            font=("Arial", 18, "bold"),
            bootstyle=WARNING
        )
        title_label.pack(pady=(0, 15))
        
        # Información del pack
        info_frame = tb.Frame(main_frame)
        info_frame.pack(fill=X, pady=(0, 15))
        
        tb.Label(info_frame, text=f"Pack ID: {pack_id}", font=("Arial", 12, "bold")).pack(anchor=W)
        tb.Label(info_frame, text=f"Comprador: {buyer}", font=("Arial", 10)).pack(anchor=W)
        tb.Label(info_frame, text=f"Pickeados: {total_picked} | Pendientes: {total_pending}", font=("Arial", 10)).pack(anchor=W)
        
        # Artículos pickeados
        if picked_by_name:
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
        
        # Artículos pendientes - LISTA DESTACADA
        if pending_by_name:
            pending_label = tb.Label(
                main_frame, 
                text=f"⏳ Artículos por pickear ({total_pending} unidades):", 
                font=("Arial", 14, "bold"),
                bootstyle=DANGER
            )
            pending_label.pack(anchor=W, pady=(15, 10))
            
            # Crear frame con scroll para la lista de pendientes
            pending_frame = tb.Frame(main_frame, bootstyle=DANGER)
            pending_frame.pack(fill=BOTH, expand=YES, padx=10, pady=5)
            
            for item_name, info in pending_by_name.items():
                # Crear frame para cada fila de artículo
                item_frame = tb.Frame(pending_frame, bootstyle=DANGER, padding=5)
                item_frame.pack(fill=X, pady=3)
                
                item_text = f"🔴 {item_name} × {info['count']}"
                tb.Label(item_frame, text=item_text, font=("Arial", 12, "bold"), bootstyle=DANGER).pack(anchor=W)
        
        # Mensaje de instrucción
        instruction_frame = tb.Frame(main_frame, bootstyle=INFO, padding=10)
        instruction_frame.pack(fill=X, pady=20)
        
        instruction_text = (
            f"⚠️ IMPORTANTE: Este pack tiene múltiples artículos.\n"
            f"Debe pickear TODOS los {total_pending} artículos restantes antes de que se imprima la etiqueta.\n"
            f"Continúe escaneando los códigos de los artículos listados arriba."
        )
        
        tb.Label(
            instruction_frame, 
            text=instruction_text, 
            font=("Arial", 11),
            justify=CENTER,
            bootstyle=INFO,
            wraplength=650
        ).pack(pady=10)
        
        # Botón cerrar
        tb.Button(
            main_frame, 
            text="Entendido - Continuar", 
            command=win.destroy,
            bootstyle=PRIMARY,
            width=25
        ).pack(pady=15)
        
        # Centrar ventana
        win.update_idletasks()
        x = (win.winfo_screenwidth() // 2) - (win.winfo_width() // 2)
        y = (win.winfo_screenheight() // 2) - (win.winfo_height() // 2)
        win.geometry(f"+{x}+{y}")

    def _show_new_sale_notification(self, new_orders):
        """Muestra popup de notificación para nuevas ventas detectadas."""
        try:
            # Crear ventana de notificación
            notification_win = tb.Toplevel(self)
            notification_win.title("🎉 ¡Nueva Venta!")
            notification_win.geometry("500x400")
            notification_win.resizable(False, False)
            notification_win.configure(bg="#2b3e50")
            
            # Centrar ventana
            notification_win.transient(self)
            notification_win.grab_set()
            
            # Frame principal
            main_frame = tb.Frame(notification_win, padding=20)
            main_frame.pack(fill="both", expand=True)
            
            # Título con emoji
            title_label = tb.Label(
                main_frame, 
                text="🎉 ¡NUEVA VENTA DETECTADA!", 
                font=("Arial", 18, "bold"),
                bootstyle="success"
            )
            title_label.pack(pady=(0, 20))
            
            # Información de las nuevas órdenes
            for order in new_orders[:3]:  # Máximo 3 órdenes para no saturar
                order_frame = tb.LabelFrame(
                    main_frame, 
                    text=f"Pedido #{order.order_id}", 
                    padding=10,
                    bootstyle="info"
                )
                order_frame.pack(fill="x", pady=5)
                
                # Mostrar artículos de la orden
                for item in order.items[:2]:  # Máximo 2 items por orden
                    # Obtener nombre del producto
                    product_name = getattr(item, 'title', item.sku) or "Producto sin nombre"
                    
                    # Obtener talle y color
                    size = getattr(item, 'size', '') or ''
                    color = getattr(item, 'color', '') or ''
                    
                    # Construir texto del artículo
                    article_text = f"📦 {product_name}"
                    details = []
                    if size:
                        details.append(f"Talle: {size}")
                    if color:
                        details.append(f"Color: {color}")
                    if details:
                        article_text += f" ({', '.join(details)})"
                    
                    # Cantidad
                    quantity = getattr(item, 'quantity', 1)
                    if quantity > 1:
                        article_text += f" × {quantity}"
                    
                    # Label del artículo
                    article_label = tb.Label(
                        order_frame,
                        text=article_text,
                        font=("Arial", 12),
                        wraplength=400
                    )
                    article_label.pack(anchor="w", pady=2)
                
                # Si hay más items, mostrar indicador
                if len(order.items) > 2:
                    more_label = tb.Label(
                        order_frame,
                        text=f"... y {len(order.items) - 2} artículo(s) más",
                        font=("Arial", 10, "italic"),
                        bootstyle="secondary"
                    )
                    more_label.pack(anchor="w", pady=2)
            
            # Si hay más órdenes, mostrar indicador
            if len(new_orders) > 3:
                more_orders_label = tb.Label(
                    main_frame,
                    text=f"... y {len(new_orders) - 3} pedido(s) más",
                    font=("Arial", 12, "italic"),
                    bootstyle="secondary"
                )
                more_orders_label.pack(pady=10)
            
            # Botón para cerrar
            def close_notification():
                notification_win.destroy()
            
            close_btn = tb.Button(
                main_frame,
                text="✅ Entendido",
                command=close_notification,
                bootstyle="success",
                width=15
            )
            close_btn.pack(pady=20)
            
            # Auto-cerrar después de 10 segundos
            def auto_close():
                try:
                    notification_win.destroy()
                except:
                    pass
            
            notification_win.after(10000, auto_close)  # 10 segundos
            
            # Hacer que la ventana parpadee para llamar la atención
            def flash_window():
                try:
                    notification_win.attributes('-topmost', True)
                    notification_win.bell()  # Sonido del sistema
                    notification_win.after(100, lambda: notification_win.attributes('-topmost', False))
                except:
                    pass
            
            flash_window()
            
        except Exception as e:
            # Si hay error en la notificación, no debe afectar el funcionamiento principal
            print(f"Error mostrando notificación: {e}")

    def _send_cancellation_to_server(self, order_id: str, reason: str):
        """Procesa cancelación usando el servicio integrado."""
        try:
            print(f"🔄 Procesando cancelación integrada:")
            print(f"   Order ID: {order_id}")
            print(f"   Motivo: {reason}")
            
            # Usar servicio integrado en lugar de POST request
            success, result = cancellation_service.cancel_order(int(order_id), reason)
            
            print(f"📡 Resultado del servicio integrado:")
            print(f"   Éxito: {success}")
            print(f"   Resultado: {result}")
            
            if success:
                print(f"✅ Cancelación procesada exitosamente")
                return True, {"status": "ok", "note": result}
            else:
                print(f"❌ Error en cancelación: {result}")
                return False, result
                
        except Exception as e:
            print(f"💥 Error en servicio de cancelación: {e}")
            return False, str(e)

    def show_daily_stats(self):
        """Muestra las estadísticas diarias en una ventana popup."""
        try:
            # Obtener estadísticas de hoy
            packages_today = get_packages_today()
            picked_today = get_picked_today()
            
            # Crear ventana de estadísticas
            stats_win = tb.Toplevel(self)
            stats_win.title("📊 Estadísticas de Hoy")
            stats_win.geometry("400x300")
            stats_win.resizable(False, False)
            
            # Centrar la ventana
            stats_win.transient(self)
            stats_win.grab_set()
            
            # Frame principal
            main_frame = tb.Frame(stats_win, padding=20)
            main_frame.pack(fill="both", expand=True)
            
            # Título
            title_label = tb.Label(
                main_frame,
                text="📊 Estadísticas de Hoy",
                font=("Arial", 16, "bold"),
                bootstyle="primary"
            )
            title_label.pack(pady=(0, 20))
            
            # Fecha actual
            today_str = datetime.now().strftime("%d/%m/%Y")
            date_label = tb.Label(
                main_frame,
                text=f"📅 {today_str}",
                font=("Arial", 12),
                bootstyle="secondary"
            )
            date_label.pack(pady=(0, 20))
            
            # Estadísticas
            stats_frame = tb.Frame(main_frame)
            stats_frame.pack(fill="x", pady=10)
            
            # Paquetes impresos
            packages_frame = tb.Frame(stats_frame, relief="solid", borderwidth=1, padding=15)
            packages_frame.pack(fill="x", pady=5)
            
            packages_icon = tb.Label(
                packages_frame,
                text="📦",
                font=("Arial", 24)
            )
            packages_icon.pack(side="left", padx=(0, 10))
            
            packages_info = tb.Frame(packages_frame)
            packages_info.pack(side="left", fill="x", expand=True)
            
            packages_title = tb.Label(
                packages_info,
                text="Paquetes Impresos",
                font=("Arial", 12, "bold")
            )
            packages_title.pack(anchor="w")
            
            packages_count = tb.Label(
                packages_info,
                text=str(packages_today),
                font=("Arial", 20, "bold"),
                bootstyle="success"
            )
            packages_count.pack(anchor="w")
            
            # Artículos pickeados
            picked_frame = tb.Frame(stats_frame, relief="solid", borderwidth=1, padding=15)
            picked_frame.pack(fill="x", pady=5)
            
            picked_icon = tb.Label(
                picked_frame,
                text="🎯",
                font=("Arial", 24)
            )
            picked_icon.pack(side="left", padx=(0, 10))
            
            picked_info = tb.Frame(picked_frame)
            picked_info.pack(side="left", fill="x", expand=True)
            
            picked_title = tb.Label(
                picked_info,
                text="Artículos Pickeados",
                font=("Arial", 12, "bold")
            )
            picked_title.pack(anchor="w")
            
            picked_count = tb.Label(
                picked_info,
                text=str(picked_today),
                font=("Arial", 20, "bold"),
                bootstyle="info"
            )
            picked_count.pack(anchor="w")
            
            # Botón cerrar
            close_btn = tb.Button(
                main_frame,
                text="✅ Cerrar",
                command=stats_win.destroy,
                bootstyle="primary",
                width=15
            )
            close_btn.pack(pady=20)
            
        except Exception as e:
            dialogs.Messagebox.show_error(
                title="Error",
                message=f"Error mostrando estadísticas: {e}"
            )
    
    def show_picked_items_today(self):
        """Muestra ventana con lista de artículos pickeados hoy."""
        try:
            picked_items = daily_cache.get_picked_items_today()
            
            # Crear ventana
            items_win = tk.Toplevel(self)
            items_win.title("Artículos Pickeados Hoy")
            items_win.geometry("600x500")
            items_win.transient(self)
            items_win.grab_set()
            
            # Frame principal con scroll
            main_frame = tb.Frame(items_win, padding=20)
            main_frame.pack(fill="both", expand=True)
            
            # Título
            title_label = tb.Label(
                main_frame,
                text="📝 Artículos Pickeados Hoy",
                font=("Arial", 16, "bold"),
                bootstyle="primary"
            )
            title_label.pack(pady=(0, 20))
            
            # Fecha actual
            today_str = datetime.now().strftime("%d/%m/%Y")
            date_label = tb.Label(
                main_frame,
                text=f"📅 {today_str} - Total: {len(picked_items)} artículos",
                font=("Arial", 12),
                bootstyle="secondary"
            )
            date_label.pack(pady=(0, 20))
            
            if not picked_items:
                no_items_label = tb.Label(
                    main_frame,
                    text="No hay artículos pickeados hoy",
                    font=("Arial", 12),
                    bootstyle="warning"
                )
                no_items_label.pack(pady=20)
            else:
                # Frame con scroll para la lista
                canvas = tk.Canvas(main_frame, height=300)
                scrollbar = ttk.Scrollbar(main_frame, orient="vertical", command=canvas.yview)
                scrollable_frame = tb.Frame(canvas)
                
                scrollable_frame.bind(
                    "<Configure>",
                    lambda e: canvas.configure(scrollregion=canvas.bbox("all"))
                )
                
                canvas.create_window((0, 0), window=scrollable_frame, anchor="nw")
                canvas.configure(yscrollcommand=scrollbar.set)
                
                # Mostrar cada artículo
                for i, item in enumerate(picked_items):
                    item_frame = tb.Frame(scrollable_frame, relief="solid", borderwidth=1, padding=10)
                    item_frame.pack(fill="x", pady=2, padx=5)
                    
                    # Hora
                    time_label = tb.Label(
                        item_frame,
                        text=item['timestamp'],
                        font=("Arial", 10),
                        bootstyle="secondary"
                    )
                    time_label.pack(anchor="w")
                    
                    # Nombre del artículo
                    title_label = tb.Label(
                        item_frame,
                        text=item['title'],
                        font=("Arial", 11, "bold")
                    )
                    title_label.pack(anchor="w")
                    
                    # SKU y Pack ID
                    details_label = tb.Label(
                        item_frame,
                        text=f"SKU: {item['sku']} | Pack: {item['pack_id']}",
                        font=("Arial", 9),
                        bootstyle="secondary"
                    )
                    details_label.pack(anchor="w")
                
                canvas.pack(side="left", fill="both", expand=True)
                scrollbar.pack(side="right", fill="y")
            
            # Botón cerrar
            close_btn = tb.Button(
                main_frame,
                text="✅ Cerrar",
                command=items_win.destroy,
                bootstyle="primary",
                width=15
            )
            close_btn.pack(pady=20)
            
        except Exception as e:
            dialogs.Messagebox.show_error(
                title="Error",
                message=f"Error mostrando artículos pickeados: {e}"
            )


# ----------------------------------------------------------------------
# Launch helper
# ----------------------------------------------------------------------

def launch_gui_v3():
    app = AppV3()
    app.mainloop()


if __name__ == "__main__":
    launch_gui_v3()
