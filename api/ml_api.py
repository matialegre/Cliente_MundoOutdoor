"""Wrappers de MercadoLibre API"""
from __future__ import annotations

import requests
from datetime import datetime
from typing import List, Dict

from utils import config
from utils.logger import get_logger
from .base import APIError

log = get_logger(__name__)

BASE_URL = "https://api.mercadolibre.com"

def refresh_access_token() -> tuple[str, str]:
    """Obtiene / refresca el access token usando el refresh token."""
    url = f"{BASE_URL}/oauth/token"
    data = {
        "grant_type": "refresh_token",
        "client_id": config.ML_CLIENT_ID,
        "client_secret": config.ML_CLIENT_SECRET,
        "refresh_token": config.ML_REFRESH_TOKEN,
    }
    resp = requests.post(url, data=data, headers={"Content-Type": "application/x-www-form-urlencoded"})
    if resp.status_code != 200:
        raise APIError.from_response(resp)
    js = resp.json()
    access_token = js["access_token"]
    seller_id = str(js["user_id"])
    return access_token, seller_id

def list_orders(seller_id: str, access_token: str, date_from: datetime, date_to: datetime) -> List[Dict]:
    offset, limit = 50, 50
    orders: list[dict] = []
    while True:
        from_str = f"{date_from.strftime('%Y-%m-%d')}T00:00:00.000-00:00"
        to_str = f"{date_to.strftime('%Y-%m-%d')}T23:59:59.000-00:00"
        url = (
            f"{BASE_URL}/orders/search?seller={seller_id}&offset={offset - limit}&limit={limit}"
            f"&order.date_created.from={from_str}&order.date_created.to={to_str}"
        )
        resp = requests.get(url, headers={"Authorization": f"Bearer {access_token}"})
        if resp.status_code != 200:
            raise APIError.from_response(resp)
        res = resp.json().get("results", [])
        if not res:
            break
        orders.extend(res)
        if len(res) < limit:
            break
        offset += limit
    return orders

# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------

def get_order_note(order_id: int | str, access_token: str) -> str:
    """Devuelve la primera nota (si existe) del pedido."""
    url = f"{BASE_URL}/orders/{order_id}/notes"
    resp = requests.get(url, headers={"Authorization": f"Bearer {access_token}"})
    if resp.status_code != 200:
        return ""
    arr = resp.json()
    if arr and arr[0].get("results"):
        return arr[0]["results"][0].get("note", "")
    return ""

def get_shipment_substatus(shipping_id: int | None, access_token: str) -> str | None:
    if not shipping_id:
        return None
    url = f"{BASE_URL}/shipments/{shipping_id}"
    resp = requests.get(url, headers={"Authorization": f"Bearer {access_token}"})
    if resp.status_code != 200:
        return None
    return resp.json().get("substatus")

def get_pack_orders(pack_id: int | str, access_token: str) -> List[int] | None:
    """Obtiene todas las order_id que pertenecen a un pack_id.
    
    Retorna una lista de order_ids o None si hay error.
    """
    if not pack_id:
        return None
    
    url = f"{BASE_URL}/packs/{pack_id}"
    resp = requests.get(url, headers={"Authorization": f"Bearer {access_token}"})
    
    if resp.status_code != 200:
        log.warning("Error obteniendo pack %s: %s", pack_id, resp.status_code)
        return None
    
    pack_data = resp.json()
    orders = pack_data.get("orders", [])
    
    # Extraer solo los IDs de las órdenes
    order_ids = [order["id"] for order in orders if "id" in order]
    
    log.debug("Pack %s contiene %d órdenes: %s", pack_id, len(order_ids), order_ids)
    return order_ids

def get_order_details(order_id: int | str, access_token: str) -> Dict | None:
    """Obtiene los detalles completos de una orden específica.
    
    Retorna el JSON de la orden o None si hay error.
    """
    if not order_id:
        return None
        
    url = f"{BASE_URL}/orders/{order_id}"
    resp = requests.get(url, headers={"Authorization": f"Bearer {access_token}"})
    
    if resp.status_code != 200:
        log.warning("Error obteniendo orden %s: %s", order_id, resp.status_code)
        return None
        
    return resp.json()
