"""Servicio que orquesta la lógica de pickeo.

La implementación está incompleta; se migrarán gradualmente las funciones del
script original aquí.
"""

from __future__ import annotations

from datetime import datetime
from functools import lru_cache
from typing import List

from utils.logger import get_logger
from utils import config
from utils import db as db_utils
from api import ml_api, dragonfish_api
import re

import io
import zipfile
import requests
import time
from models.order import Order
import re

log = get_logger(__name__)

class PickerService:
    def __init__(self) -> None:
        # state for pick session
        self._session_active: bool = False
        # unidades pendientes: (Order, item_index, unit_idx)
        self._pending_units: list[tuple[Order, int, int]] = []
        # unidades pickeadas
        self._picked_units: list[tuple[Order, int, int]] = []
        self.access_token: str | None = None
        self.seller_id: str | None = None
        self.orders: list[Order] = []

    def _ensure_token(self) -> None:
        if self.access_token is None:
            self.access_token, self.seller_id = ml_api.refresh_access_token()
            log.info("Access token refrescado; seller_id=%s", self.seller_id)

    # ------------------------------------------------------------------
    # Utilidades
    # ------------------------------------------------------------------
    @staticmethod
    def _barcode_from_sku(sku: str | None) -> str | None:
        """Genera código de barras para patrones Sunset/Ombak.
        SKU esperado: "01/1602" (o con guiones). Devuelve 13 dígitos o None.
        Lógica: 0 + color(2) + modelo(4) + 100 + última cifra de modelo + 00
        Ej: 01/1602 -> 0 01 1602 100 2 00 -> 0011602100200
        """
        if not sku:
            return None
        digits = re.sub(r"\D", "", sku)
        if len(digits) != 6:
            return None
        color = digits[:2]
        modelo = digits[2:]
        return f"0{color}{modelo}100{modelo[-1]}00"

    # ------------------------------------------------------------------
    def load_orders(self, date_from: datetime, date_to: datetime) -> List[Order]:
        self._ensure_token()
        raw_orders = ml_api.list_orders(self.seller_id, self.access_token, date_from, date_to)
        # Crear objetos Order
        self.orders = [Order.from_api(o) for o in raw_orders]

        # Enriquecer con nota, substatus real y completar barcodes
        for ord_obj in self.orders:
            note = ml_api.get_order_note(ord_obj.id, self.access_token)
            if note:
                ord_obj.notes = note
            real_sub = ml_api.get_shipment_substatus(ord_obj.shipping_id, self.access_token)
            if real_sub:
                ord_obj.shipping_substatus = real_sub
            # Completar barcode si falta y patrón coincide
            for it in ord_obj.items:
                if not it.barcode:
                    # intentar SQL
                    _, cb_sql = self.get_ml_code_from_barcode(it.sku or "")
                    if cb_sql:
                        it.barcode = cb_sql
                    else:
                        it.barcode = self._barcode_from_sku(it.sku)

        log.info("%d pedidos cargados", len(self.orders))
        return self.orders

    # --- Métodos auxiliares migrated del script monolítico ---

    def get_ml_code_from_barcode(self, barcode: str) -> tuple[str | None, str | None]:
        """Devuelve (codigo_ml, codigo_barra_real) para un código de barras físico.

        Consulta la base SQL (Dragonfish depósito). Regresa (None, None) si no se encuentra.
        """
        query = (
            "SELECT RTRIM(equi.CCOLOR) AS CODIGO_COLOR, RTRIM(equi.CTALLE) AS CODIGO_TALLE, "
            "RTRIM(equi.CARTICUL) AS CODIGO_ARTICULO, RTRIM(equi.CCODIGO) AS CODIGO_BARRA, "
            "RTRIM(c_art.ARTDES) AS ARTDES "
            "FROM DRAGONFISH_DEPOSITO.ZooLogic.EQUI AS equi "
            "LEFT JOIN DRAGONFISH_DEPOSITO.ZooLogic.ART AS c_art ON equi.CARTICUL = c_art.ARTCOD "
            "WHERE RTRIM(equi.CCODIGO) = ?"
        )
        row = db_utils.fetchone(query, [barcode])
        if not row:
            return None, None
        codigo_ml = f"{row.CODIGO_ARTICULO}-{row.CODIGO_COLOR}-{row.CODIGO_TALLE}"
        return codigo_ml, row.CODIGO_BARRA

    def get_article_data(self, barcode: str) -> dict | None:
        """Obtiene datos extendidos del artículo para enviar a Dragonfish."""
        query = (
            "SELECT RTRIM(equi.CCOLOR) AS CODIGO_COLOR, RTRIM(equi.CTALLE) AS CODIGO_TALLE, "
            "RTRIM(equi.CARTICUL) AS CODIGO_ARTICULO, RTRIM(equi.CCODIGO) AS CODIGO_BARRA, "
            "RTRIM(c_art.ARTDES) AS ARTDES "
            "FROM DRAGONFISH_DEPOSITO.ZooLogic.EQUI AS equi "
            "LEFT JOIN DRAGONFISH_DEPOSITO.ZooLogic.ART AS c_art ON equi.CARTICUL = c_art.ARTCOD "
            "WHERE RTRIM(equi.CCODIGO) = ?"
        )
        row = db_utils.fetchone(query, [barcode])
        if not row:
            return None
        return {
            "CODIGO_ARTICULO": row.CODIGO_ARTICULO,
            "CODIGO_COLOR": row.CODIGO_COLOR,
            "CODIGO_TALLE": row.CODIGO_TALLE,
            "CODIGO_BARRA": row.CODIGO_BARRA,
            "ARTDES": row.ARTDES,
        }

    def send_stock_movement(self, pedido_id: int | str, barcode: str, cantidad: int = 1) -> tuple[bool, str]:
        """Envía movimiento de stock a la API Dragonfish."""
        datos = self.get_article_data(barcode) or {}
        return dragonfish_api.send_stock_movement(pedido_id, barcode, cantidad, datos)

    # --- Ejemplo de flujo simplificado: imprimir etiqueta de un envío ---

    def download_label_zpl(self, shipping_id: int) -> bytes | None:
        """Descarga la etiqueta ZPL con hasta 5 reintentos."""
        self._ensure_token()
        ids_str = str(shipping_id)
        url = (
            f"https://api.mercadolibre.com/shipment_labels?shipment_ids={ids_str}&response_type=zpl2"
        )
        retries = getattr(config, "ML_LABEL_RETRIES", 5)
        delay_s = getattr(config, "ML_LABEL_DELAY_S", 3)
        for intento in range(1, retries + 1):
            try:
                resp = requests.get(url, headers={"Authorization": f"Bearer {self.access_token}"}, timeout=10)
                if resp.status_code == 200:
                    zip_data = io.BytesIO(resp.content)
                    with zipfile.ZipFile(zip_data, 'r') as zf:
                        for name in zf.namelist():
                            if name.endswith(('.txt', '.zpl')):
                                log.info("Etiqueta descargada OK intento %s/%s", intento, retries)
                                return zf.read(name)
                    log.error("Zip sin archivo ZPL - intento %s/%s", intento, retries)
                else:
                    log.warning("Label FAIL intento %s/%s – status %s – cuerpo: %s", intento, retries, resp.status_code, resp.text[:300])
            except requests.RequestException as e:
                log.error("Label EXC intento %s/%s: %s", intento, retries, e)
            time.sleep(delay_s)
        log.error("Etiqueta no pudo descargarse tras %s intentos", retries)
        return None

    # ------------------------------------------------------------------
    # Pick workflow helpers
    # ------------------------------------------------------------------
    def start_pick_session(self, orders: list[Order]) -> None:
        """Inicializa la lista de unidades pendientes a pickear."""
        self._pending_units.clear()
        self._picked_units.clear()
        self.orders = orders
        for ord_obj in orders:
            for idx, it in enumerate(ord_obj.items):
                # Completar sku faltante a partir del código de barras
                # Completar sku faltante o barcode faltante
                if not it.sku and it.barcode:
                    sku_db, _ = self.get_ml_code_from_barcode(it.barcode)
                    if sku_db:
                        it.sku = sku_db
                # Regla Sunset/Ombak: SKU corto '01/1601' -> barcode '0' + digits + '100100'
                if it.sku and not it.barcode:
                    digits = re.sub(r'\D', '', it.sku)
                    if len(digits) == 6:  # 2+4
                        color = digits[:2]
                        modelo = digits[2:]
                        it.barcode = f"0{color}{modelo}100{modelo[-1]}00"
                for unit_idx in range(it.quantity):
                    self._pending_units.append((ord_obj, idx, unit_idx))

    def scan_barcode(self, barcode: str) -> tuple[bool, str]:
        """Procesa un escaneo.
        Devuelve (ok, mensaje). Si ok=True el artículo coincide y se marca como pickeado.
        """
        codigo_ml, codigo_barra_real = self.get_ml_code_from_barcode(barcode)
        if not codigo_ml:
            return False, f"Código {barcode} no encontrado en SQL"

        # Buscar un pending unit cuyo SKU coincida
        for tup in self._pending_units:
            ord_obj, idx_item, unit_idx = tup
            it = ord_obj.items[idx_item]
            norm = lambda s: (s or '').replace('-', '').replace('_', '').replace('/', '').replace(' ', '').upper()
            if norm(it.sku) == norm(codigo_ml) or norm(it.barcode) == norm(codigo_barra_real):
                self._pending_units.remove(tup)
                self._picked_units.append(tup)

                try:
                    ok_stock, msg_stock = self.send_stock_movement(ord_obj.pack_id or ord_obj.id, codigo_barra_real or barcode, 1)
                except Exception as e:
                    ok_stock = False
                    msg_stock = str(e)
                    log.error("Error send_stock_movement: %s", e)

                # Cuando ya se pickearon todas las unidades de un pedido, imprimir etiqueta
                label_ok = False
                pack_id = ord_obj.pack_id or ord_obj.id
                pending_same_pack = [p for p in self._pending_units if (p[0].pack_id or p[0].id) == pack_id]
                log.debug("Pack %s: pendientes %d", pack_id, len(pending_same_pack))
                if not pending_same_pack and ord_obj.shipping_id:
                    try:
                        self.print_shipping_label(ord_obj.shipping_id)
                        label_ok = True
                    except Exception:
                        pass
                msg_ok = f"Pick OK – {codigo_ml}"
                if label_ok:
                    msg_ok += " | Etiqueta impresa"
                if ok_stock:
                    msg_ok += " | Stock OK"
                else:
                    msg_ok += f" | Stock {msg_stock}"
                return True, msg_ok
        # Construir lista de SKUs pendientes (máx 5 para debug)
        pending_skus = []
        for ord_obj, idx_item, _ in self._pending_units:
            it = ord_obj.items[idx_item]
            if it.sku:
                pending_skus.append(it.sku)
            if len(pending_skus) >= 5:
                break
        pend_str = ", ".join(pending_skus) if pending_skus else "(sin pendientes)"
        return False, f"{codigo_ml} no coincide. Pendientes: {pend_str}"

    # ------------------------------------------------------------------
    # Impresión
    # ------------------------------------------------------------------
    def print_shipping_label(self, shipping_id: int) -> None:
        from printing.zpl_printer import print_zpl
        zpl = self.download_label_zpl(shipping_id)
        if zpl:
            print_zpl(zpl)
        else:
            log.error("No se pudo descargar/imprimir la etiqueta para shipping_id=%s", shipping_id)

