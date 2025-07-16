"""Widgets compuestos para la GUI: lista de pedidos con botones Copiar / FALLADO.

Esto permite mantener `app_gui.py` relativamente limpio.
"""
from __future__ import annotations

import tkinter as tk
from tkinter import ttk, WORD, messagebox
from typing import List, Callable, Any
from datetime import datetime, timezone, timedelta
from tkinter.constants import LEFT, RIGHT

# Alias para mantener compatibilidad
tb = ttk

from models.order import Order, Item

# Callback types
CopyCallback = Callable[[str], None]
FalladoCallback = Callable[[Order], None]


class OrderList(tk.Frame):
    """Scroll-area con un Frame que contiene los pedidos y sus ítems."""

    def __init__(self, master: tk.Widget, copy_cb: CopyCallback, fallado_cb: FalladoCallback):
        super().__init__(master)
        self._copy_cb = copy_cb
        self._fallado_cb = fallado_cb
        self._orders: List[Order] = []
        self._order_frames: list[tk.Frame] = []
        self._canvas = None  # Store canvas reference

        # Canvas con scroll
        self._canvas = tk.Canvas(self, bd=0, highlightthickness=0, bg='#2b2b2b')
        vscroll = ttk.Scrollbar(self, orient="vertical", command=self._canvas.yview)
        self._inner = tk.Frame(self._canvas, bg='#2b2b2b')
        
        # Configurar el scroll
        self._canvas.configure(yscrollcommand=vscroll.set, bg='#2b2b2b')
        vscroll.pack(side="right", fill="y")
        self._canvas.pack(side="left", fill="both", expand=True)
        self._canvas.create_window((0, 0), window=self._inner, anchor="nw", tags=("inner",))
        
        # Configurar el scroll con el mouse
        def _on_mousewheel(event):
            self._canvas.yview_scroll(int(-1*(event.delta/120)), "units")
        
        def _on_frame_configure(event=None):
            self._canvas.configure(scrollregion=self._canvas.bbox("all"))
            
        self._inner.bind("<Configure>", _on_frame_configure)
        self._canvas.bind("<Configure>", lambda e: self._canvas.itemconfig("inner", width=e.width))
        self._canvas.bind_all("<MouseWheel>", _on_mousewheel)

    # ---------------------------------------------------------------------
    # Public API
    # ---------------------------------------------------------------------

    def _norm(self, txt: str | None) -> str:
        return txt.replace('-', '').replace('_', '').replace('/', '').replace(' ', '').upper() if txt else ''

    def _scroll_to_frame(self, idx: int) -> None:
        if idx >= len(self._order_frames):
            return
        fr = self._order_frames[idx]
        self._canvas.update_idletasks()
        y = fr.winfo_rooty() - self._canvas.winfo_rooty()
        self._canvas.yview_moveto(y / max(1, self._inner.winfo_height()))
        fr.configure(highlightthickness=3, highlightbackground='#ffea00')

    def scroll_to_order(self, search: int | str) -> bool:
        """Busca por ID (con o sin #), SKU o código de barras y hace scroll."""
        s_raw = str(search).lstrip('#').strip()
        s_norm = self._norm(s_raw)

        # 1) Buscar por ID / Pack ID
        for idx, o in enumerate(self._orders):
            if str(o.pack_id or o.id) == s_raw:
                self._scroll_to_frame(idx)
                return True

        # 2) Buscar por SKU o Código de barra normalizados
        for idx, o in enumerate(self._orders):
            for it in o.items:
                if self._norm(it.sku or '') == s_norm or self._norm(it.barcode or '') == s_norm:
                    self._scroll_to_frame(idx)
                    return True

        # 3) Buscar en nombre de comprador
        s_upper = s_raw.upper()
        for idx, o in enumerate(self._orders):
            if s_upper in (o.buyer or '').upper():
                self._scroll_to_frame(idx)
                return True

        # 4) Buscar palabra en la descripción del ítem (case-insensitive)
        for idx, o in enumerate(self._orders):
            for it in o.items:
                if s_upper in (it.title or '').upper():
                    self._scroll_to_frame(idx)
                    return True
        return False

    def highlight_order(self, order_id: int | str, color: str = "violet") -> None:
        """Cambia el color de fondo del pedido indicado (pack_id o id)."""
        for idx, o in enumerate(self._orders):
            oid = str(o.pack_id or o.id)
            if oid == str(order_id).lstrip('#') and idx < len(self._order_frames):
                fr = self._order_frames[idx]
                fr.configure(bg=color)
                for child in fr.winfo_children():
                    child.configure(bg=color)
                break

    def set_orders(self, orders: List[Order]) -> None:
        # Calcular packs duplicados para resaltar
        pack_counts = {}
        for o in orders:
            pid = o.pack_id or o.id
            pack_counts[pid] = pack_counts.get(pid, 0) + 1
        self._dup_packs = {pid for pid, cnt in pack_counts.items() if cnt > 1}

        for fr in self._order_frames:
            fr.destroy()
        self._order_frames.clear()
        self._orders = orders
        for o in orders:
            self._render_order(o)

    # ---------------------------------------------------------------------
    # Internals
    # ---------------------------------------------------------------------

    def _render_order(self, o: Order) -> None:
        """Renderiza un pedido en el canvas."""
        # Determinar color de fondo según estado
        text_color = "#ffffff"  # texto blanco
        
        status_raw = (o.shipping_substatus or '').lower()
        # Estilos según estado
        if status_raw == 'printed':
            color_bg = '#1b5e20'  # verde oscuro
        elif status_raw == 'ready_to_print' or status_raw == '':
            color_bg = '#5c1b1b'  # rojo oscuro
        else:
            color_bg = '#2b2b2b'  

        # Border azul si el pack tiene múltiples pedidos
        border_color = '#1976d2' if (o.pack_id or o.id) in getattr(self, '_dup_packs', set()) else color_bg
        pedido_fr = tk.Frame(
            self._inner,
            padx=2,
            pady=2,
            relief="groove",
            borderwidth=2,
            bg=color_bg,
            highlightthickness=2,
            highlightbackground=border_color,
        )
        pedido_fr.pack(fill="x", pady=3, padx=4)
        self._order_frames.append(pedido_fr)

        # --- Cabecera con número de pedido y substatus --------------------
        # Cabecera del pedido
        head = tk.Frame(pedido_fr, bg=color_bg)
        head.pack(fill="x", pady=(0, 2))

        # Nombre comprador
        tk.Label(head, text=o.buyer.upper(), font=("Arial", 10, "bold"), bg=color_bg, fg="#ffcc80").pack(side=LEFT, padx=(0,8))
        # Número de pedido
        ent_num = tk.Entry(head, width=18, font=("Arial", 12, "bold"), 
                         relief="flat", bg=color_bg, fg="#4dabf7",
                         readonlybackground=color_bg, borderwidth=0)
        ent_num.insert(0, f"#{o.pack_id or o.id}")
        ent_num.configure(state="readonly")
        ent_num.pack(side=LEFT, padx=(4, 2), pady=2)

        # Botón Copiar
        tk.Button(head, text="📋 Copiar", 
                command=lambda v=o.pack_id or o.id: self._copy_cb(str(v)),
                bg="#4a4a4a", fg="white", bd=0, padx=8, pady=1,
                font=("Arial", 8, "bold")).pack(side=LEFT, padx=2)

        # Substatus
        status_raw = (o.shipping_substatus or '').lower()
        if status_raw == 'printed':
            sub_txt = 'IMPRESA'
            status_bg = '#2e7d32'  # verde oscuro
            status_fg = '#a5d6a7'
        elif status_raw == 'ready_to_print':
            sub_txt = 'LISTA PARA IMPRIMIR'
            status_bg = '#c62828'  # rojo fuerte
            status_fg = '#ffebee'
        else:
            if status_raw == '':
                sub_txt = 'LISTA PARA IMPRIMIR'
            else:
                sub_txt = status_raw.upper()
            status_bg = '#4a4a4a'
            status_fg = '#ffffff'
            
        lbl_status = tk.Label(head, text=sub_txt.upper(), 
                            font=("Arial", 8, "bold"), 
                            bg=status_bg, fg=status_fg,
                            padx=6, pady=1)
        lbl_status.pack(side=RIGHT, padx=4, pady=2)

        # Nota (si la hay)
        if o.notes:
            tk.Label(head, text=f"📌 {o.notes[:60]}" + ("…" if len(o.notes) > 60 else ""), 
                   fg="#ffd54f", font=("Arial", 9, "italic"), 
                   bg=color_bg, wraplength=400, justify="right").pack(side=RIGHT, padx=8)

        # --- Ítems ---------------------------------------------------------
        for it in o.items:
            self._render_item(pedido_fr, it, color_bg, o)


    def _render_item(self, parent: tk.Frame, it: Item, color_bg: str, order: Order):
        # Frame principal del ítem
        row = tk.Frame(parent, bg=color_bg, padx=0, pady=2)
        row.pack(fill="x", padx=8, pady=1)
        
        # Línea 1: Descripción del producto
        desc_frame = tk.Frame(row, bg=color_bg)
        desc_frame.pack(fill="x", pady=(0, 2))
        
        # Botones de acción
        btn_frame = tk.Frame(desc_frame, bg=color_bg)
        btn_frame.pack(side=RIGHT, padx=(10, 0))
        
        # Botón Copiar
        tk.Button(btn_frame, text="📋", 
                 command=lambda v=it.title: self._copy_cb(v),
                 bg="#4a4a4a", fg="white", bd=0, 
                 font=("Arial", 8, "bold"),
                 padx=6, pady=1).pack(side=LEFT, padx=1)
        
        # Botón Fallado
        tk.Button(btn_frame, text="❌ FALLADO", 
                 command=lambda o=order: self._fallado_cb(o),
                 bg="#c62828", fg="white", 
                 font=("Arial", 8, "bold"),
                 padx=6, pady=1, bd=0).pack(side=LEFT, padx=1)
        
        # Campo de texto con descripción
        ent_name = tk.Text(desc_frame, height=2, width=50, 
                          font=("Arial", 9), 
                          bg=color_bg, fg="#e0e0e0",
                          relief="flat", wrap=WORD,
                          padx=4, pady=2, borderwidth=0)
        ent_name.insert("1.0", it.title or "Sin título")
        ent_name.configure(state="disabled")
        ent_name.pack(side=LEFT, fill="x", expand=True)
        
        # Línea 2: Información adicional
        info_frame = tk.Frame(row, bg=color_bg)
        info_frame.pack(fill="x", padx=8)
        
        # Cantidad y SKU
        qty_text = f"Cant: {it.quantity}"
        tk.Label(info_frame, text=qty_text, 
                font=("Arial", 8, "bold"), 
                bg=color_bg, fg="#a5d6a7").pack(side=LEFT, padx=(0, 10))

        sku_raw = it.sku or it.barcode or ''
        if sku_raw:
            sku_norm = sku_raw.replace('-', '').replace('_', '')
            tk.Label(info_frame, text=f"SKU: {sku_norm}",
                     font=("Arial", 8, "bold"),
                     bg=color_bg, fg="#80cbc4").pack(side=LEFT, padx=(0, 10))
        if it.barcode:
            tk.Label(info_frame, text=f"CB: {it.barcode}", 
                    font=("Arial", 8), 
                    bg=color_bg, fg="#c5e1a5").pack(side=LEFT, padx=(0, 15))
        
        # Shipping ID (si existe)
        if order.shipping_id:
            ship_text = f"Envío: {order.shipping_id}"
            tk.Label(info_frame, text=ship_text, 
                    font=("Arial", 8), 
                    bg=color_bg, fg="#b39ddb").pack(side=LEFT)
        
        # Separador
        sep = tk.Frame(row, height=1, bg="#444444")
        sep.pack(fill="x", pady=4, padx=8)
