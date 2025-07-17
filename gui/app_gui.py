"""GUI principal usando ttkbootstrap

Muestra los pedidos en un canvas con scroll, cada uno con:
• botón Copiar pedido
• descripción de cada ítem con botón Copiar y botón FALLADO
• nota, Shipping ID, SKU, cantidad, substatus

Botón Pickear abre la ventana de pickeo libre. Escanea códigos de barras,
marca unidades pickeadas, imprime etiqueta y envía movimientos de stock.
"""

from __future__ import annotations

from datetime import datetime, timezone, timedelta
import ttkbootstrap as tb
from ttkbootstrap.constants import *

import tkinter as tk
from services.picker_service import PickerService
from printing.zpl_printer import print_zpl
import ttkbootstrap.dialogs as dialogs
from gui.order_widgets import OrderList

DEFAULT_PRINTER = None
KEYWORDS_NOTE = ['DEPO', 'MUNDOAL', 'MTGBBL', 'BBPS', 'MONBAHIA', 'MTGBBPS']


class App(tb.Window):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs, title="Cliente Matías", themename="darkly")
        self.service = PickerService()

        self._build_widgets()

    def _build_widgets(self) -> None:
        frm = tb.Frame(self, padding=10)
        frm.pack(fill=BOTH, expand=YES)

        # Fechas por defecto: ayer y hoy
        today = datetime.now()
        yesterday = today - timedelta(days=1)
        
        self.var_from = tk.StringVar(value=yesterday.strftime("%d/%m/%Y"))
        self.var_to = tk.StringVar(value=today.strftime("%d/%m/%Y"))

        tb.Label(frm, text="Fecha Desde (DD/MM/YYYY)", font=('Arial', 9, 'bold')).grid(row=0, column=0, sticky=W, pady=2)
        self.entry_from = tb.Entry(frm, width=14, textvariable=self.var_from, font=('Arial', 10))
        self.entry_from.grid(row=0, column=1, padx=5, pady=2, sticky=W)

        tb.Label(frm, text="Fecha Hasta (DD/MM/YYYY)", font=('Arial', 9, 'bold')).grid(row=1, column=0, sticky=W, pady=2)
        self.entry_to = tb.Entry(frm, width=14, textvariable=self.var_to, font=('Arial', 10))
        self.entry_to.grid(row=1, column=1, padx=5, pady=2, sticky=W)

        # filtros
        filter_frame = tb.Frame(frm)
        filter_frame.grid(row=2, column=0, columnspan=2, sticky=W, pady=5)
        self.var_include_printed = tk.BooleanVar(value=False)
        self.var_until_13 = tk.BooleanVar(value=False)
        tb.Checkbutton(filter_frame, text="Incluir impresos", variable=self.var_include_printed).pack(side=LEFT, padx=5)
        tb.Checkbutton(filter_frame, text="Hasta 13:00", variable=self.var_until_13).pack(side=LEFT, padx=5)

        # --- Buscar por Pedido/Pack ID ---
        search_frame = tb.Frame(frm)
        search_frame.grid(row=3, column=0, columnspan=2, sticky=W, pady=(5,2))
        tk.Label(search_frame, text="Buscar ID:").pack(side=LEFT)
        self.var_search_id = tk.StringVar()
        ent_search = tb.Entry(search_frame, textvariable=self.var_search_id, width=22)
        ent_search.pack(side=LEFT, padx=5)
        def do_search(event=None):
            val = self.var_search_id.get().strip()
            if not val:
                return
            if not self.order_list.scroll_to_order(val):
                dialogs.Messagebox.show_info(f"ID {val} no encontrado")
        tb.Button(search_frame, text="Ir", bootstyle=PRIMARY, command=do_search).pack(side=LEFT)
        ent_search.bind('<Return>', do_search)

        btn_frame = tb.Frame(frm)
        btn_frame.grid(row=4, column=0, columnspan=2, pady=10, sticky=EW)
        self.btn_pick = tb.Button(btn_frame, text="Pickear", bootstyle=WARNING, command=self.open_pick_window)
        self.btn_pick.pack(side=LEFT, padx=5)
        tb.Button(btn_frame, text="Cargar Pedidos", bootstyle=SUCCESS, command=self.load_orders).pack(side=LEFT, padx=5)
        tb.Button(btn_frame, text="Elegir impresora", bootstyle=SECONDARY, command=self.choose_printer).pack(side=LEFT, padx=5)
        tb.Button(btn_frame, text="Test Impresión", bootstyle=INFO, command=self.test_printer).pack(side=LEFT, padx=5)

        # --- Lista de pedidos scrollable ---
        self.order_list = OrderList(frm, copy_cb=self.copy_to_clip, fallado_cb=self.mark_failed)
        self.order_list.grid(row=5, column=0, columnspan=2, sticky=NSEW)
        frm.rowconfigure(5, weight=1)
        frm.columnconfigure(1, weight=1)

        # status bar
        self.status = tb.Label(self, text="", bootstyle=SECONDARY)
        self.status.pack(side=BOTTOM, fill=X)

    def choose_printer(self):
        import win32print
        global DEFAULT_PRINTER
        printers = [p[2] for p in win32print.EnumPrinters(win32print.PRINTER_ENUM_LOCAL | win32print.PRINTER_ENUM_CONNECTIONS)]
        if not printers:
            dialogs.Messagebox.show_error("No se encontraron impresoras")
            return
        dlg = tk.Toplevel(self)
        dlg.title("Seleccione impresora")
        lb = tk.Listbox(dlg, height=10, width=60)
        for pr in printers:
            lb.insert(END, pr)
        lb.pack(padx=10, pady=10)
        def select():
            sel = lb.curselection()
            if sel:
                from utils import config
                config.PRINTER_NAME = lb.get(sel[0])
                dialogs.Messagebox.show_info(f"Impresora seleccionada: {config.PRINTER_NAME}")
                dlg.destroy()
        tk.Button(dlg, text="Seleccionar", command=select).pack(pady=5)

    def test_printer(self):
        from utils import config
        sample_zpl = "^XA^CF0,50^FO50,50^FDTEST OK^FS^XZ"
        print_zpl(sample_zpl, printer_name=config.PRINTER_NAME)
        dialogs.Messagebox.show_info("Etiqueta de prueba enviada")

    def copy_to_clip(self, text: str):
        self.clipboard_clear()
        self.clipboard_append(text)

    def mark_failed(self, order):
        dialogs.Messagebox.show_info("FALLADO presionado — lógica pendiente")

    # Treeview double-click no longer used
    def on_row_double(self, event):
        item_id = self.tree.focus()
        if not item_id:
            return
        vals = self.tree.item(item_id, "values")
        shipping_id = vals[3]
        if not shipping_id:
            dialogs.Messagebox.show_error("Pedido sin shipping_id")
            return
        self.service.print_shipping_label(int(shipping_id))
        dialogs.Messagebox.show_info("Etiqueta enviada a imprimir")

    def open_pick_window(self):
        if not self.service.orders:
            dialogs.Messagebox.show_error("Primero cargue pedidos")
            return
        # Inicializar sesión
        self.service.start_pick_session(self.service.orders)

        win = tk.Toplevel(self)
        win.title("Pickeo Libre")
        tk.Label(win, text="Escanee código de barras:").pack(pady=8)
        ent = tk.Entry(win)
        ent.pack(padx=20, pady=8)
        ent.focus()
        lbl_msg = tk.Label(win, text="", font=('Arial', 10, 'bold'))
        lbl_msg.pack(pady=4)

        def process():
            code = ent.get().strip()
            if not code:
                return
            ok, msg = self.service.scan_barcode(code)
            lbl_msg.configure(text=msg, fg='#00e676' if ok else '#ff5252')
            # Limpiar mensaje luego de 3 s
            win.after(3000, lambda: lbl_msg.configure(text=""))

            if ok:
                pack_id = self.service.last_pack_id
                pend = self.service.pending_units_summary(pack_id) if pack_id else []
                if pend:  # aún faltan unidades del mismo pack
                    dialogs.Messagebox.show_info("Pendientes", "Faltan:\n" + "\n".join(pend))
                    # resaltar pedidos pendientes en azul
                    for o in self.service.orders:
                        if (o.pack_id or o.id) == pack_id:
                            self.order_list.highlight_order(o.pack_id or o.id, color='#2196f3')
                else:      # pack completo
                    self.order_list.highlight_order(pack_id, color='violet')

            ent.delete(0, END)
        ent.bind('<Return>', lambda e: process())

    def load_orders(self) -> None:
        date_from_str = self.entry_from.get()
        date_to_str = self.entry_to.get()
        try:
            df = datetime.strptime(date_from_str, "%d/%m/%Y")
            dt = datetime.strptime(date_to_str, "%d/%m/%Y")
        except ValueError:
            tb.dialogs.Messagebox.show_error("Formato de fecha inválido")
            return

        # Mostrar estado de carga y cambiar cursor
        self.status.configure(text="Cargando pedidos...", bootstyle=INFO)
        self.configure(cursor="watch")
        self.update_idletasks()

        import threading
        def worker():
            try:
                orders = self.service.load_orders(df, dt)
                self.after(0, lambda: self._on_orders_loaded(orders))
            except Exception as e:
                self.after(0, lambda: self._on_orders_error(e))
        threading.Thread(target=worker, daemon=True).start()

    def _on_orders_loaded(self, orders):
        self.status.configure(text="Pedidos cargados OK", bootstyle=SUCCESS)
        # Filtrar y mostrar en lista (reutilizar lógica existente)
        self._filter_and_show_orders(orders)
        self.configure(cursor="")

    def _on_orders_error(self, err):
        self.status.configure(text=str(err), bootstyle=DANGER)
        tb.dialogs.Messagebox.show_error(str(err))
        self.configure(cursor="")

    def _filter_and_show_orders(self, orders):
        # Rango de fechas ingresado por el usuario
        try:
            date_from = datetime.strptime(self.var_from.get(), "%d/%m/%Y").date()
            date_to   = datetime.strptime(self.var_to.get(), "%d/%m/%Y").date()
        except ValueError:
            date_from = date_to = None

        allow_printed = self.var_include_printed.get()
        filtered = []
        for o in orders:
            # ----- filtro por fecha local -----
            try:
                dt_created = datetime.fromisoformat(
                    o.date_created.replace('Z', '+00:00')
                ).astimezone(timezone(timedelta(hours=-3)))
            except Exception:
                dt_created = None
            if date_from and date_to and dt_created:
                if not (date_from <= dt_created.date() <= date_to):
                    continue
            # ----- filtro hasta las 13:00 -----
            if self.var_until_13.get() and dt_created and dt_created.hour >= 13:
                continue
            # ----- filtro por nota -----
            note_up = (o.notes or '').upper()
            if not any(k in note_up for k in KEYWORDS_NOTE):
                continue
            # =========================================================
            #  ÚNICO filtro que nos interesa:
            #  solo substatus "ready_to_print"; permitir "printed"
            #  únicamente si el usuario marcó "incluir impresos".
            # =========================================================
            sub = (o.shipping_substatus or '').lower()
            if sub == 'ready_to_print':
                filtered.append(o)
                continue
            if allow_printed and sub == 'printed':
                filtered.append(o)
                continue
            # todo lo demás se descarta (incluye 'en camino')
            continue
        self.order_list.set_orders(filtered)



def launch_gui() -> None:
    app = App()
    app.mainloop()
