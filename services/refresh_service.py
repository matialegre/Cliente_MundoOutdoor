"""Hilo que refresca periódicamente la lista de pedidos sin bloquear la GUI."""
from __future__ import annotations

import threading
import time
import tkinter as tk
from tkinter import messagebox
from typing import Callable, Optional


class OrderRefresher(threading.Thread):
    """Refresca pedidos en segundo plano con back-off exponencial."""

    def __init__(self, picker_service, state, intervalo: int = 120, notification_callback: Optional[Callable] = None):
        super().__init__(daemon=True)
        self._picker = picker_service
        self._state = state
        self._intervalo = intervalo  # Cambio a 2 minutos (120 segundos)
        self._notification_callback = notification_callback
        self._previous_order_ids = set()  # Para detectar nuevas ventas
        self._first_run = True

    def run(self) -> None:
        while True:
            try:
                # Cargar órdenes actuales
                current_orders = self._picker.load_orders_cached()
                self._state.visibles = current_orders
                
                # Detectar nuevas ventas (solo después del primer run)
                if not self._first_run:
                    current_order_ids = {order.id for order in current_orders}
                    new_order_ids = current_order_ids - self._previous_order_ids
                    
                    # Si hay nuevas ventas, mostrar notificación
                    if new_order_ids and self._notification_callback:
                        new_orders = [order for order in current_orders if order.id in new_order_ids]
                        self._notification_callback(new_orders)
                    
                    self._previous_order_ids = current_order_ids
                else:
                    # En el primer run, solo guardar los IDs existentes
                    self._previous_order_ids = {order.id for order in current_orders}
                    self._first_run = False
                
                # reset intervalo
                self._intervalo = 120  # 2 minutos
            except Exception as exc:
                self._state.mensajes.append(("error", str(exc)))
                # back-off hasta 10 min
                self._intervalo = min(self._intervalo * 2, 600)
            time.sleep(self._intervalo)
