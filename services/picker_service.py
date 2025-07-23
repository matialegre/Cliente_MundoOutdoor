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
from utils.sku_resolver import SKUResolver, is_out_sku
from utils import db as db_utils
from api import ml_api, dragonfish_api
import re

import io
import zipfile
import requests
import time
from models.order import Order
import re
from utils.daily_stats import increment_packages_today, increment_picked_today
from utils.daily_cache import daily_cache
from services.parallel_picker import ParallelPickProcessor

log = get_logger(__name__)

class PickerService:
    def __init__(self) -> None:
        # state for pick session
        self._session_active: bool = False
        self._pending_units: list[tuple[Order, int, int]] = []
        self._picked_units: list[tuple[Order, int, int]] = []
        self.access_token: str | None = None
        self.seller_id: str | None = None
        self.orders: list[Order] = []
        self.last_pack_id: str | None = None   # ← línea añadida
        self.parallel_processor: ParallelPickProcessor | None = None   # ← procesador paralelo
        self._cache_expiry: float = 0
        self._already_discounted: set[tuple[str, str]] = set()
        self._already_printed: set[str] = set()  # Track printed items to avoid restock discount
        self._last_from: datetime | None = None
        self._last_to: datetime | None = None

    def _ensure_token(self) -> None:
        if self.access_token is None:
            self.access_token, self.seller_id = ml_api.refresh_access_token()
            self.sku_resolver = SKUResolver(self.access_token)
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
        
        # Expandir packs multiventa
        expanded_orders = []
        pack_ids_processed = set()
        
        for raw_order in raw_orders:
            pack_id = raw_order.get("pack_id")
            
            if pack_id and pack_id not in pack_ids_processed:
                # Es un pack, obtener todas las órdenes del pack
                pack_order_ids = ml_api.get_pack_orders(pack_id, self.access_token)
                
                if pack_order_ids and len(pack_order_ids) > 1:
                    # Pack multiventa: cargar cada orden individualmente
                    log.info("🔗 Pack multiventa %s encontrado con %d órdenes", pack_id, len(pack_order_ids))
                    
                    for order_id in pack_order_ids:
                        order_details = ml_api.get_order_details(order_id, self.access_token)
                        if order_details:
                            expanded_orders.append(order_details)
                    
                    pack_ids_processed.add(pack_id)
                else:
                    # Pack con una sola orden o error al obtener órdenes
                    expanded_orders.append(raw_order)
                    if pack_id:
                        pack_ids_processed.add(pack_id)
            elif not pack_id:
                # Orden sin pack
                expanded_orders.append(raw_order)
            # Si pack_id ya fue procesado, saltar esta orden (ya está incluida)
        
        # Crear objetos Order
        self.orders = [Order.from_api(o) for o in expanded_orders]
        # actualizar caché
        self._cache_expiry = time.time() + 60
        self._last_from = date_from
        self._last_to = date_to

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
        
        # Resolver SKUs reales para productos con sufijo OUT
        self._resolve_real_skus()

        log.info("%d pedidos cargados (expandidos desde packs)", len(self.orders))
        return self.orders
    
    def _resolve_real_skus(self):
        """Resuelve SKUs reales para productos con sufijo OUT usando la API de ML."""
        if not self.sku_resolver:
            log.warning("⚠️ SKU resolver no inicializado")
            return
        
        out_items_count = 0
        resolved_count = 0
        
        for ord_obj in self.orders:
            for item in ord_obj.items:
                if is_out_sku(item.sku):
                    out_items_count += 1
                    original_sku = item.sku
                    
                    # Resolver SKU real usando item_id y variation_id
                    real_sku = self.sku_resolver.get_real_sku(
                        item.item_id, 
                        item.variation_id, 
                        original_sku
                    )
                    
                    if real_sku != original_sku:
                        log.info(f"🔄 SKU resuelto: {original_sku} → {real_sku}")
                        item.real_sku = real_sku  # Guardar el SKU real
                        resolved_count += 1
                    else:
                        item.real_sku = original_sku  # Mantener el original si no se pudo resolver
                else:
                    # Para items sin sufijo OUT, el SKU real es el mismo
                    item.real_sku = item.sku
        
        if out_items_count > 0:
            log.info(f"✅ SKUs procesados: {out_items_count} con OUT, {resolved_count} resueltos")
        else:
            log.debug("ℹ️ No se encontraron SKUs con sufijo OUT")

    # ------------------------------------------------------------------
    def load_orders_cached(self):
        """Devuelve los pedidos desde caché si no expiró (TTL 60 s)."""
        if time.time() < self._cache_expiry:
            return self.orders
        if self._last_from is None or self._last_to is None:
            raise ValueError("load_orders_cached aún no inicializado; llamar load_orders primero")
        return self.load_orders(self._last_from, self._last_to)

    # --- Métodos auxiliares migrated del script monolítico ---

    def normalize_barcode(self, barcode: str) -> str:
        """Normaliza un código de barras removiendo guiones y espacios para comparación."""
        return barcode.replace('-', '').replace(' ', '').upper().strip()
    
    def get_ml_code_from_barcode(self, barcode: str) -> tuple[str | None, str | None]:
        """Devuelve (codigo_ml, codigo_barra_real) para un código de barras físico.
        
        OPTIMIZADO: Usa cache diario para evitar consultas SQL repetidas.
        """
        # Verificar cache primero
        cached_result = daily_cache.get_barcode_info(barcode)
        if cached_result is not None:
            if cached_result == "NOT_FOUND":
                return None, None
            return cached_result[0], cached_result[1]
        
        # Mapeo manual para códigos especiales que no están en SQL
        BARCODE_MANUAL_MAP = {
            "7798333733209": "APCAGB01--",
            "101-350-ML": "101-W350-WML",
            "NMIDKUDZDWNN038": "NMIDKUDZDW-NNO-T38",
            "NMIDKTDZHVNN039": "NMIDKTDZHV-NC0-T39",
            "NMIDKUDZDWMTP40": "NMIDKUHZDY-NNO-T40",
        }
        
        # Verificar mapeo manual primero
        if barcode in BARCODE_MANUAL_MAP:
            sku_manual = BARCODE_MANUAL_MAP[barcode]
            result = (sku_manual, barcode)
            daily_cache.set_barcode_info(barcode, result)
            return sku_manual, barcode
        
        # Consulta SQL
        if barcode.count('-') == 2:
            art, col, tal = barcode.split('-', 2)
            query = (
                "SELECT RTRIM(equi.CCOLOR) AS CODIGO_COLOR, RTRIM(equi.CTALLE) AS CODIGO_TALLE, "
                "RTRIM(equi.CARTICUL) AS CODIGO_ARTICULO, RTRIM(equi.CCODIGO) AS CODIGO_BARRA, "
                "RTRIM(c_art.ARTDES) AS ARTDES "
                "FROM DRAGONFISH_DEPOSITO.ZooLogic.EQUI AS equi "
                "LEFT JOIN DRAGONFISH_DEPOSITO.ZooLogic.ART AS c_art ON equi.CARTICUL = c_art.ARTCOD "
                "WHERE RTRIM(equi.CARTICUL) = ? AND RTRIM(equi.CCOLOR) = ? AND RTRIM(equi.CTALLE) = ?"
            )
            params = [art, col, tal]
        else:
            query = (
                "SELECT RTRIM(equi.CCOLOR) AS CODIGO_COLOR, RTRIM(equi.CTALLE) AS CODIGO_TALLE, "
                "RTRIM(equi.CARTICUL) AS CODIGO_ARTICULO, RTRIM(equi.CCODIGO) AS CODIGO_BARRA, "
                "RTRIM(c_art.ARTDES) AS ARTDES "
                "FROM DRAGONFISH_DEPOSITO.ZooLogic.EQUI AS equi "
                "LEFT JOIN DRAGONFISH_DEPOSITO.ZooLogic.ART AS c_art ON equi.CARTICUL = c_art.ARTCOD "
                "WHERE RTRIM(equi.CCODIGO) = ?"
            )
            params = [barcode]
        
        row = db_utils.fetchone(query, params)
        
        if not row:
            # Guardar en cache como no encontrado
            daily_cache.set_barcode_info(barcode, "NOT_FOUND")
            return None, None
        else:
            # Construir SKU y guardar en cache
            codigo_articulo = row[2]
            codigo_color = row[0]
            codigo_talle = row[1]
            sku_construido = f"{codigo_articulo}-{codigo_color}-{codigo_talle}"
            result = (sku_construido, barcode)
            daily_cache.set_barcode_info(barcode, result)
            return sku_construido, barcode
            return sku_construido, barcode
    
    def get_ml_code_from_barcode_reverse(self, sku: str) -> tuple[str | None, str | None]:
        """Busca código de barra por SKU (operación inversa).
        
        Args:
            sku: SKU a buscar
            
        Returns:
            tuple[barcode, sku] si se encuentra, (None, None) si no
        """
        # Verificar mapeo manual primero (inverso)
        BARCODE_MANUAL_MAP = {
            "7798333733209": "APCAGB01--",  # Anafe Portatil Camping
            "101-350-ML": "101-W350-WML",  # Chaleco Weis
            "NMIDKUDZDWNN038": "NMIDKUDZDW-NN0-T38",  # Zapatilla 1
            "NMIDKTDZHVNN039": "NMIDKTDZHV-NC0-T39",  # Zapatilla 2
            "NMIDKUDZDWMTP40": "NMIDKUHZDY-NN0-T40",  # Zapatilla 3
        }
        
        # Buscar en mapeo manual (inverso)
        for barcode, mapped_sku in BARCODE_MANUAL_MAP.items():
            if mapped_sku == sku:
                log.debug(f"📋 SKU {sku} encontrado en mapeo manual con código: {barcode}")
                return barcode, sku
        
        # Consulta SQL para buscar por SKU
        query = (
            "SELECT RTRIM(equi.CCODIGO) AS CODIGO_BARRA "
            "FROM DRAGONFISH_DEPOSITO.ZooLogic.EQUI equi "
            "INNER JOIN DRAGONFISH_DEPOSITO.ZooLogic.ART c_art ON equi.CARTICUL = c_art.ARTCOD "
            "WHERE CONCAT(RTRIM(equi.CARTICUL), '-', RTRIM(equi.CCOLOR), '-', RTRIM(equi.CTALLE)) = ?"
        )
        
        try:
            log.info(f"🔍 [REVERSE] Buscando SKU '{sku}' en base de datos SQL...")
            row = db_utils.fetchone(query, [sku])
            
            if row:
                barcode = row[0]
                log.info(f"✅ [REVERSE] SKU '{sku}' ENCONTRADO en BD con código: {barcode}")
                return barcode, sku
            else:
                log.warning(f"❌ [REVERSE] SKU '{sku}' NO encontrado en BD")
                return None, None
                
        except Exception as e:
            log.error(f"❌ [REVERSE] Error buscando SKU {sku} en BD: {e}")
            return None, None

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

    # --------------------------------------------------------------
    #  Resumen de unidades pendientes por pack
    # --------------------------------------------------------------
    def pending_units_summary(self, pack_id: str) -> list[str]:
        """Devuelve lista de strings 'SKU – unidad X/Y' aún pendientes en pack_id."""
        summary: list[str] = []
        for ord_obj, idx_item, unit_idx in self._pending_units:
            if (ord_obj.pack_id or ord_obj.id) != pack_id:
                continue
            item = ord_obj.items[idx_item]
            summary.append(f"{item.sku} – {unit_idx + 1}/{item.quantity}")
        return summary
    
    def get_pending_items_for_pack(self, pack_id: str) -> list[str]:
        """Devuelve lista de SKUs únicos pendientes en un pack."""
        pending_skus = set()
        for ord_obj, idx_item, unit_idx in self._pending_units:
            if (ord_obj.pack_id or ord_obj.id) == pack_id:
                item = ord_obj.items[idx_item]
                if item.sku:
                    pending_skus.add(item.sku)
        return sorted(list(pending_skus))
    

    
    def get_pack_items_summary(self, pack_id: str) -> dict:
        """Devuelve resumen completo de items en un pack (pickeados y pendientes)."""
        picked_items = {}
        pending_items = {}
        
        # Contar items pickeados
        for ord_obj, idx_item, unit_idx in self._picked_units:
            if (ord_obj.pack_id or ord_obj.id) == pack_id:
                item = ord_obj.items[idx_item]
                sku = item.sku or 'Sin SKU'
                picked_items[sku] = picked_items.get(sku, 0) + 1
        
        # Contar items pendientes
        for ord_obj, idx_item, unit_idx in self._pending_units:
            if (ord_obj.pack_id or ord_obj.id) == pack_id:
                item = ord_obj.items[idx_item]
                sku = item.sku or 'Sin SKU'
                pending_items[sku] = pending_items.get(sku, 0) + 1
        
        return {
            'picked': picked_items,
            'pending': pending_items,
            'total_picked': sum(picked_items.values()),
            'total_pending': sum(pending_items.values())
        }


    def send_stock_movement(self, pedido_id: int | str, barcode: str, cantidad: int = 1) -> tuple[bool, str]:
        """Envía movimiento de stock a la API Dragonfish."""
        datos = self.get_article_data(barcode) or {}
        return dragonfish_api.send_stock_movement(pedido_id, barcode, cantidad, datos)

    # --- Ejemplo de flujo simplificado: imprimir etiqueta de un envío ---

    def download_label_zpl(self, shipping_id: int) -> bytes | None:
        """Descarga la etiqueta ZPL con hasta 5 reintentos."""
        log.info("Iniciando descarga de etiqueta ZPL para shipping_id=%s", shipping_id)
        
        self._ensure_token()
        log.debug("Token de acceso verificado: %s", "OK" if self.access_token else "FALTA")
        
        ids_str = str(shipping_id)
        url = (
            f"https://api.mercadolibre.com/shipment_labels?shipment_ids={ids_str}&response_type=zpl2"
        )
        log.debug("URL de descarga: %s", url)
        
        retries = getattr(config, "ML_LABEL_RETRIES", 5)
        delay_s = getattr(config, "ML_LABEL_DELAY_S", 3)
        log.debug("Configuración: %d reintentos, %d segundos de delay", retries, delay_s)
        
        for intento in range(1, retries + 1):
            log.info("Intento %d/%d de descarga de etiqueta", intento, retries)
            try:
                resp = requests.get(url, headers={"Authorization": f"Bearer {self.access_token}"}, timeout=10)
                log.debug("Respuesta HTTP: status=%d, content-length=%d", resp.status_code, len(resp.content) if resp.content else 0)
                
                if resp.status_code == 200:
                    log.debug("Respuesta exitosa, procesando ZIP...")
                    zip_data = io.BytesIO(resp.content)
                    
                    try:
                        with zipfile.ZipFile(zip_data, 'r') as zf:
                            files_in_zip = zf.namelist()
                            log.debug("Archivos en ZIP: %s", files_in_zip)
                            
                            for name in files_in_zip:
                                if name.endswith(('.txt', '.zpl')):
                                    zpl_content = zf.read(name)
                                    log.info("Etiqueta ZPL encontrada: %s (%d bytes)", name, len(zpl_content))
                                    log.debug("Contenido ZPL (primeros 200 chars): %s", zpl_content[:200])
                                    return zpl_content
                            log.error("ZIP descargado pero sin archivo ZPL válido - intento %s/%s", intento, retries)
                    except zipfile.BadZipFile as e:
                        log.error("Archivo ZIP corrupto en intento %s/%s: %s", intento, retries, e)
                        log.debug("Contenido recibido (primeros 200 bytes): %s", resp.content[:200])
                else:
                    log.warning("Descarga FALLIDA intento %s/%s – HTTP %s", intento, retries, resp.status_code)
                    log.debug("Cuerpo de respuesta: %s", resp.text[:500])
                    
            except requests.RequestException as e:
                log.error("Excepción de red en intento %s/%s: %s", intento, retries, e)
            
            if intento < retries:
                log.debug("Esperando %d segundos antes del siguiente intento...", delay_s)
                time.sleep(delay_s)
                
        log.error("FALLO TOTAL: Etiqueta no pudo descargarse tras %s intentos para shipping_id=%s", retries, shipping_id)
        return None

    # ------------------------------------------------------------------
    # Pick workflow helpers
    # ------------------------------------------------------------------
    def start_pick_session(self, orders: list[Order]):
        """Inicializa la lista de unidades pendientes a pickear."""
        log.info("Iniciando sesión de picking con %d órdenes", len(orders))
        
        self._session_active = True
        self._pending_units.clear()
        self._picked_units.clear()
        self.orders = orders
        
        # Inicializar procesador paralelo
        self.parallel_processor = ParallelPickProcessor(self)
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
        log.info("Procesando escaneo de código: %s", barcode)
        
        # Obtener código ML con verificación de None
        result = self.get_ml_code_from_barcode(barcode)
        if result is None:
            return False, f"Código {barcode} no encontrado en SQL"
            
        codigo_ml, codigo_barra_real = result
        if not codigo_ml:
            return False, f"Código {barcode} no encontrado en SQL"

        log.debug("Código encontrado: ML=%s, Barcode=%s", codigo_ml, codigo_barra_real)
        
        # PRIMERO: Verificar si es un pedido ya impreso para reimpresión
        # FILTRAR Y PRIORIZAR órdenes para evitar seleccionar órdenes viejas/incorrectas
        norm = lambda s: (s or '').replace('-', '').replace('_', '').replace('/', '').replace(' ', '').upper()
        
        # Buscar todas las órdenes que coincidan con el código (incluyendo SKU real)
        matching_orders = []
        for ord_obj in self.orders:
            for it in ord_obj.items:
                # Buscar por SKU original, SKU real, o código de barras (normalizado)
                sku_normalized = self.normalize_barcode(it.sku or '')
                real_sku_normalized = self.normalize_barcode(getattr(it, 'real_sku', '') or '')
                barcode_normalized = self.normalize_barcode(it.barcode or '')
                codigo_ml_normalized = self.normalize_barcode(codigo_ml or '')
                codigo_barra_normalized = self.normalize_barcode(codigo_barra_real or '')
                
                sku_match = (sku_normalized == codigo_ml_normalized or 
                           real_sku_normalized == codigo_ml_normalized)
                barcode_match = (barcode_normalized == codigo_barra_normalized or
                               barcode_normalized == codigo_ml_normalized or
                               sku_normalized == codigo_barra_normalized)
                
                if sku_match or barcode_match:
                    matching_orders.append(ord_obj)
                    log.debug("🎯 Coincidencia encontrada: orden %s, item %s (SKU: %s, Real: %s, Barcode: %s)", 
                             ord_obj.id, it.title[:30], it.sku, getattr(it, 'real_sku', 'N/A'), it.barcode)
                    break  # Solo necesitamos una coincidencia por orden
        
        if matching_orders:
            log.info("🔍 Encontradas %d órdenes con código %s", len(matching_orders), codigo_ml)
            
            # FILTRAR por depósito correcto (todos los KEYWORDS_NOTE válidos)
            KEYWORDS_NOTE = ['DEPO', 'MUNDOAL', 'MTGBBL', 'BBPS', 'MONBAHIA', 'MTGBBPS']
            filtered_orders = []
            for ord_obj in matching_orders:
                note_up = (ord_obj.notes or '').upper()
                # Verificar si contiene alguno de los keywords de depósito válidos
                has_valid_depot = any(keyword in note_up for keyword in KEYWORDS_NOTE)
                if has_valid_depot:
                    filtered_orders.append(ord_obj)
                else:
                    log.debug("🚫 Orden %s descartada - depósito incorrecto: %s", ord_obj.id, ord_obj.notes)
            
            if not filtered_orders:
                log.warning("⚠️ Ninguna orden tiene depósito válido (%s)", ', '.join(KEYWORDS_NOTE))
                return False, f"Código {codigo_ml} encontrado pero no corresponde a depósitos válidos"
            
            # PRIORIZAR por estado: ready_to_print > printed > otros
            def order_priority(ord_obj):
                sub = (ord_obj.shipping_substatus or '').lower()
                if sub == 'ready_to_print':
                    return 1  # Máxima prioridad
                elif sub == 'printed':
                    return 2  # Segunda prioridad
                else:
                    return 3  # Menor prioridad
            
            # Ordenar por prioridad (menor número = mayor prioridad)
            filtered_orders.sort(key=order_priority)
            selected_order = filtered_orders[0]
            
            sub = (selected_order.shipping_substatus or '').lower()
            pack_id = selected_order.pack_id or selected_order.id
            
            log.info("✅ Orden seleccionada: %s (estado: %s, depósito: %s)", 
                    pack_id, sub, selected_order.notes)
            
            if sub == 'printed' and selected_order.shipping_id:
                log.info("REIMPRESIÓN: Pedido %s ya impreso, solo reimprimiendo etiqueta", pack_id)
                try:
                    success, msg = self.print_shipping_label_with_retries(selected_order.shipping_id)
                    if success:
                        return True, f"ARTÍCULO YA IMPRESO – Etiqueta reimpresa (Pack: {pack_id}) | STOCK IGUAL"
                    else:
                        return False, f"Error reimprimir etiqueta: {msg}"
                except Exception as e:
                    log.error("Error en reimpresión para pack %s: %s", pack_id, e)
                    return False, "Error reimprimir etiqueta"
        
        # SEGUNDO: Buscar en pending units (flujo normal)
        log.debug("No encontrado en pedidos impresos, buscando en pendientes...")
        for tup in self._pending_units:
            ord_obj, idx_item, unit_idx = tup
            it = ord_obj.items[idx_item]
            # Normalizar códigos para comparación (con y sin guiones)
            sku_normalized = self.normalize_barcode(it.sku or '')
            barcode_normalized = self.normalize_barcode(it.barcode or '')
            codigo_ml_normalized = self.normalize_barcode(codigo_ml or '')
            codigo_barra_normalized = self.normalize_barcode(codigo_barra_real or '')
            
            if (sku_normalized == codigo_ml_normalized or 
                barcode_normalized == codigo_barra_normalized or
                sku_normalized == codigo_barra_normalized or
                barcode_normalized == codigo_ml_normalized):
                pack_id = ord_obj.pack_id or ord_obj.id
                substatus_lower = (ord_obj.shipping_substatus or '').lower()
                
                # FLUJO CORREGIDO: Verificar si el pack estará completo ANTES de hacer cambios
                self.last_pack_id = pack_id
                pending_same_pack_before = [p for p in self._pending_units if (p[0].pack_id or p[0].id) == pack_id and p != tup]
                will_be_complete = len(pending_same_pack_before) == 0
                
                # NUEVO FLUJO PARALELO: Hacer picking y luego procesar en paralelo
                self._pending_units.remove(tup)
                self._picked_units.append(tup)
                
                # Verificar si necesitamos procesar (no ya impreso)
                if substatus_lower == 'printed':
                    # Ya impreso: solo confirmar sin procesar
                    log.info(f"📋 Artículo ya impreso: {pack_id} - {codigo_ml}")
                    msg_ok = f"Pick OK – {codigo_ml} | STOCK IGUAL | YA IMPRESO"
                    
                    # Incrementar contador de artículos pickeados
                    try:
                        increment_picked_today(1)
                        log.debug("🎯 Contador de artículos pickeados incrementado")
                    except Exception as e:
                        log.warning(f"⚠️ Error incrementando contador de pickeados: {e}")
                    
                    return True, msg_ok
                
                # Verificar si ya se descontó stock hoy
                unit_index = unit_idx
                if daily_cache.is_stock_already_discounted(pack_id, codigo_ml, unit_index):
                    log.info(f"⚠️ Stock ya descontado hoy para {pack_id} - {codigo_ml} (unidad {unit_index})")
                    msg_ok = f"Pick OK – {codigo_ml} | STOCK YA DESCONTADO HOY"
                    
                    # Incrementar contador de artículos pickeados
                    try:
                        increment_picked_today(1)
                        log.debug("🎯 Contador de artículos pickeados incrementado")
                    except Exception as e:
                        log.warning(f"⚠️ Error incrementando contador de pickeados: {e}")
                    
                    return True, msg_ok
                
                # Verificar si el pack estará completo para decidir si imprimir
                pending_same_pack_after = [p for p in self._pending_units if (p[0].pack_id or p[0].id) == pack_id]
                pack_will_be_complete = len(pending_same_pack_after) == 0
                
                # PROCESAMIENTO PARALELO
                if pack_will_be_complete and ord_obj.shipping_id:
                    # Pack completo: procesar stock e impresión en paralelo
                    log.info(f"🚀 PACK COMPLETO - Procesando en paralelo: {pack_id}")
                    
                    if self.parallel_processor:
                        success, parallel_msg = self.parallel_processor.process_parallel(
                            pack_id, 
                            codigo_ml, 
                            codigo_barra_real or barcode, 
                            ord_obj.shipping_id
                        )
                        
                        # Marcar como descontado preventivamente (el thread lo confirmará)
                        daily_cache.mark_stock_discounted(pack_id, codigo_ml, unit_index)
                        
                        # Incrementar contadores
                        try:
                            increment_picked_today(1)
                            increment_packages_today(1)
                            log.debug("📦🎯 Contadores incrementados")
                        except Exception as e:
                            log.warning(f"⚠️ Error incrementando contadores: {e}")
                        
                        # Agregar al historial
                        try:
                            item_title = it.title or codigo_ml or "Artículo sin nombre"
                            daily_cache.add_picked_item(pack_id, codigo_ml, item_title)
                            log.debug(f"📝 Artículo agregado al historial: {item_title}")
                        except Exception as e:
                            log.warning(f"⚠️ Error agregando al historial: {e}")
                        
                        return True, f"Pick OK – {codigo_ml} | 🚀 PROCESANDO EN PARALELO"
                    else:
                        log.error("❌ Procesador paralelo no inicializado")
                        return False, "Error: Procesador paralelo no disponible"
                else:
                    # Pack incompleto: solo descontar stock (sin impresión)
                    log.info(f"📦 Pack incompleto - Solo descontando stock: {pack_id}")
                    
                    try:
                        ok_stock, msg_stock = self.send_stock_movement(pack_id, codigo_barra_real or barcode, 1)
                        if ok_stock:
                            # Marcar como descontado en cache diario
                            daily_cache.mark_stock_discounted(pack_id, codigo_ml, unit_index)
                            
                            # Mostrar información de progreso
                            total_discounted = daily_cache.get_stock_discount_count(pack_id, codigo_ml)
                            msg_stock = f"STOCK DESCONTADO - Unidad {unit_index + 1} ({total_discounted} descontadas)"
                            log.info(f"✅ Stock descontado: {pack_id} - {codigo_ml} (unidad {unit_index})")
                        else:
                            log.error(f"❌ Fallo descuento de stock: {pack_id} - {codigo_ml} - {msg_stock}")
                            
                        # Incrementar contador de artículos pickeados
                        try:
                            increment_picked_today(1)
                            log.debug("🎯 Contador de artículos pickeados incrementado")
                        except Exception as e:
                            log.warning(f"⚠️ Error incrementando contador de pickeados: {e}")
                            
                    except Exception as e:
                        ok_stock = False
                        msg_stock = f"Error crítico: {str(e)}"
                        log.error(f"🔥 Excepción en descuento: {pack_id} - {codigo_ml} - {e}", exc_info=True)
                
                # Para packs incompletos: mostrar qué falta
                if not pack_will_be_complete:
                    pending_items = self.get_pending_items_for_pack(pack_id)
                    total_pending = len(pending_same_pack_after)
                    log.info("🔗 PACK %s INCOMPLETO: %d artículos pendientes", pack_id, total_pending)
                    
                    if len(pending_items) <= 3:
                        msg_ok = f"Pick OK – {codigo_ml} | 🔗 PACK INCOMPLETO - Faltan: {', '.join(pending_items)}"
                    else:
                        msg_ok = f"Pick OK – {codigo_ml} | 🔗 PACK INCOMPLETO - Faltan: {', '.join(pending_items[:3])} y {len(pending_items) - 3} más"
                    
                    msg_ok += f" ({total_pending} unidades pendientes)"
                    
                    # Agregar información de stock
                    if 'ok_stock' in locals() and ok_stock:
                        msg_ok += " | Stock OK"
                    elif 'msg_stock' in locals():
                        msg_ok += f" | {msg_stock}"
                    
                    return True, msg_ok
                
        # Si llegamos aquí, no se encontró coincidencia en pending_units
        # Construir lista de nombres de productos pendientes (máx 5 para mostrar)
        pending_products = []
        for ord_obj, idx_item, _ in self._pending_units:
            it = ord_obj.items[idx_item]
            # Usar el nombre del producto en lugar del SKU
            product_name = it.title or it.sku or "Producto sin nombre"
            
            # Agregar detalles de talle/color si existen
            talle = getattr(it, 'size', '') or getattr(it, 'talle', '')
            color = getattr(it, 'color', '')
            
            if talle and color:
                product_name += f" (T:{talle}, C:{color})"
            elif talle:
                product_name += f" (T:{talle})"
            elif color:
                product_name += f" (C:{color})"
            
            pending_products.append(product_name)
            if len(pending_products) >= 5:
                break
        
        # FALLBACK: Buscar en seller_custom_field antes de dar "NO ESTÁ PARA PICKEAR"
        log.debug("🔍 FALLBACK: Buscando código %s en seller_custom_field de órdenes filtradas", codigo_ml)
        custom_field_matches = []
        
        for ord_obj in self.orders:
            for it in ord_obj.items:
                # Buscar en real_sku (que viene del seller_custom_field)
                real_sku = getattr(it, 'real_sku', '')
                if real_sku and norm(real_sku) == norm(codigo_ml):
                    custom_field_matches.append((ord_obj, it))
                    log.info("🎯 FALLBACK: Coincidencia en custom field - Orden %s, SKU real: %s", ord_obj.id, real_sku)
        
        if custom_field_matches:
            # Encontró coincidencia en custom field - mostrar alerta de posibilidad de cruce
            match_order, match_item = custom_field_matches[0]  # Tomar la primera coincidencia
            product_name = match_item.title or match_item.sku or "Producto sin nombre"
            
            log.warning("⚠️ POSIBILIDAD DE CRUCE detectada para código %s", codigo_ml)
            return False, f"⚠️ POSIBILIDAD DE CRUCE\n\nCódigo: {codigo_ml}\nProducto: {product_name}\nOrden: {match_order.id}\n\n¿Confirmar picking? (Presiona Enter para continuar o ESC para cancelar)"
        
        # No se encontró ni en SKUs normales ni en custom fields
        # Crear mensaje con nombres de productos en filas
        if pending_products:
            pend_str = "\n• " + "\n• ".join(pending_products)
            log.debug("Código %s no encontrado en pendientes ni custom fields. Productos pendientes: %s", codigo_ml, [p[:50] for p in pending_products])
            return False, f"NO ESTÁ PARA PICKEAR\n\nProductos pendientes:{pend_str}"
        else:
            log.debug("Código %s no encontrado en pendientes ni custom fields. Sin productos pendientes", codigo_ml)
            return False, "NO ESTÁ PARA PICKEAR"

    # ------------------------------------------------------------------
    # Impresión
    # ------------------------------------------------------------------
    def print_shipping_label(self, shipping_id: int) -> None:
        """Función original de impresión (mantenida por compatibilidad)."""
        from printing.zpl_printer import print_zpl
        log.info("Iniciando impresión de etiqueta para shipping_id=%s", shipping_id)
        
        try:
            zpl = self.download_label_zpl(shipping_id)
            if zpl:
                log.info("ZPL descargado exitosamente, tamaño: %d bytes", len(zpl) if isinstance(zpl, (str, bytes)) else 0)
                log.debug("Contenido ZPL (primeros 200 chars): %s", str(zpl)[:200] if zpl else "None")
                
                print_zpl(zpl)
                log.info("Etiqueta enviada a impresora exitosamente")
            else:
                log.error("No se pudo descargar la etiqueta ZPL para shipping_id=%s", shipping_id)
        except Exception as e:
            log.error("Error en print_shipping_label para shipping_id=%s: %s", shipping_id, str(e))
            raise
    
    def print_shipping_label_with_retries(self, shipping_id: int, max_attempts: int = 3) -> tuple[bool, str]:
        """Imprime etiqueta con reintentos y validación mejorada.
        
        Returns:
            tuple[bool, str]: (success, message)
        """
        from printing.zpl_printer import print_zpl
        
        log.info("🖨️ Iniciando impresión con reintentos para shipping_id=%s (máx %d intentos)", shipping_id, max_attempts)
        
        for attempt in range(1, max_attempts + 1):
            try:
                log.info("📥 Intento %d/%d - Descargando etiqueta ZPL...", attempt, max_attempts)
                
                # Descargar ZPL
                zpl = self.download_label_zpl(shipping_id)
                if not zpl:
                    log.warning("⚠️ Intento %d/%d - No se pudo descargar ZPL", attempt, max_attempts)
                    if attempt < max_attempts:
                        log.info("⏳ Esperando 2 segundos antes del siguiente intento...")
                        time.sleep(2)
                        continue
                    else:
                        return False, "No se pudo descargar la etiqueta ZPL"
                
                log.info("✅ ZPL descargado exitosamente, tamaño: %d bytes", len(zpl) if isinstance(zpl, (str, bytes)) else 0)
                log.debug("📄 Contenido ZPL (primeros 200 chars): %s", str(zpl)[:200] if zpl else "None")
                
                # Intentar imprimir
                log.info("🖨️ Intento %d/%d - Enviando a impresora ZEBRA...", attempt, max_attempts)
                print_zpl(zpl)
                
                # Verificar que la impresión fue exitosa
                log.info("✅ Etiqueta enviada a impresora exitosamente en intento %d/%d", attempt, max_attempts)
                
                # Pequeña pausa para asegurar que la impresión se procese
                time.sleep(1)
                
                return True, f"Impresión exitosa (intento {attempt}/{max_attempts})"
                
            except Exception as e:
                log.error("❌ Error en intento %d/%d para shipping_id=%s: %s", attempt, max_attempts, shipping_id, str(e))
                
                if attempt < max_attempts:
                    log.info("⏳ Esperando 3 segundos antes del siguiente intento...")
                    time.sleep(3)
                else:
                    log.error("💥 FALLO TOTAL: No se pudo imprimir tras %d intentos", max_attempts)
                    return False, f"Error tras {max_attempts} intentos: {str(e)}"
        
        return False, "Error desconocido en impresión"

