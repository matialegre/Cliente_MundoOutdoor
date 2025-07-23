"""Servicio para marcar artículos fallados y revertir stock."""
from __future__ import annotations

import requests
from api import dragonfish_api

WEBHOOK_URL = "http://190.211.201.217:5001/cancel"


def cancel_item(order_id: int, sku: str, motivo: str = "FALLADO") -> None:
    """Envía un webhook y registra un movimiento de stock de ingreso.

    Parámetros
    ----------
    order_id: int
        ID del pedido (o pack_id) afectado.
    sku: str
        Código ML del artículo.
    motivo: str
        Texto para la nota en ML / sistemas.
    """
    try:
        requests.post(
            WEBHOOK_URL,
            json={"order_id": order_id, "reason": motivo},
            timeout=10,
        )
    except Exception as exc:
        # El webhook es informativo, no abortar si falla
        print(f"Error enviando webhook cancel_item: {exc}")

    # 1 = ingreso en depósito
    dragonfish_api.send_stock_movement(order_id, sku, 1, tipo=1)
