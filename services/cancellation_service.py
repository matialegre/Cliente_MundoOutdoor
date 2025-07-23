#!/usr/bin/env python3
"""
Servicio de cancelación integrado - Reemplaza el servidor externo
Maneja cancelaciones de órdenes ML con recálculo de depósito ganador
"""
from __future__ import annotations

import json
import re
import time
import requests
import pyodbc
from datetime import datetime
from collections import defaultdict
from pathlib import Path
from typing import Dict, Tuple, Optional
from requests import HTTPError

from utils.logger import get_logger

log = get_logger(__name__)

class CancellationService:
    """Servicio integrado para cancelaciones ML con recálculo de depósito ganador."""
    
    # ───────── CONFIG ───────────────────────────────────────────────
    CLIENT_ID = "5057564940459485"
    CLIENT_SECRET = "NM0wSta1bSNSt4CxSEOeSwRC2p9eHQD7"
    TOKEN_URL = "https://api.mercadolibre.com/oauth/token"
    
    TOKEN_PATH = Path(r"C:\Users\Mundo Outdoor\Desktop\Develop_Mati\Escritor Meli\token.json")
    
    # ───────── PRIORIDADES INTEGRADAS ─────────────────────────────
    # Prioridades integradas directamente (no depende de archivos externos)
    PRIORIDADES_PUNTOS = {
        "DEP": 10000000,
        "MDQ": 0,
        "MONBAHIA": 6000000,
        "MTGBBPS": 4000000,
        "MTGCBA": 0,
        "MTGCOM": 0,
        "MTGJBJ": 0,
        "MTGROCA": 3000000,
        "MUNDOAL": 8000000,
        "MUNDOCAB": 20000000,
        "MUNDOROC": 2000000,
        "NQNALB": 1000000,
        "NQNSHOP": 0,
    }
    
    PRIORIDADES_MULT = {
        "DEP": 1.0,
        "MDQ": 0.5,
        "MONBAHIA": 0.8,
        "MTGBBPS": 0.8,
        "MTGCBA": 1.0,
        "MTGCOM": 1.0,
        "MTGJBJ": 1.0,
        "MTGROCA": 0.5,
        "MUNDOAL": 0.8,
        "MUNDOCAB": 5.0,
        "MUNDOROC": 0.2,
        "NQNALB": 0.2,
        "NQNSHOP": 0.3,
    }
    
    PRIORIDADES_MULTIPLICADORES = {
        "DEP": 1,
        "MDQ": 1,
        "MONBAHIA": 1,
        "MTGBBPS": 1,
        "MTGCBA": 1,
        "MTGCOM": 1,
        "MTGJBJ": 1,
        "MTGROCA": 1,
        "MUNDOAL": 1,
        "MUNDOCAB": 1,
        "MUNDOROC": 1,
        "NQNALB": 1,
        "NQNSHOP": 1,
    }
    
    # Mapeo de depósitos para notas de cancelación
    DEPOS_MAP = {
        "DEP": "BBLANCADE",
        "DEPO": "BBLANCADEPO", 
        "DEPOSITO": "BBLANCADEPO",
        "MDQ": "MARDELGUEM",
        "MONBAHIA": "BBLANCA",
        "MTGBBPS": "BBLANCA",
        "MTGCBA": "CORDOBA",
        "MTGCOM": "NEUQUENCOMAHUE",
        "MTGJBJ": "MARDELJUAN",
        "MTGROCA": "RIONEGROMT",
        "MUNDOAL": "BBLANCAMUN",
        "MUNDOCAB": "PALERMO",
        "MUNDOROC": "RIONEGROMD",
        "NQNALB": "NEUQUENCENTRO",
        "NQNSHOP": "NEUQUENANONIMA",
    }
    
    CONN_STR = (
        r"DRIVER={ODBC Driver 17 for SQL Server};"
        r"SERVER=ranchoaspen\zoo2025;"
        r"DATABASE=master;"
        r"Trusted_Connection=yes;"
    )
    
    BASES_EXCLUIDAS = {"MELI", "ADMIN", "WOO", "TN", "OUTLET", "MTGCBA", "MDQ", "MTGJBJ"}
    MAX_LEN = 240
    CONNECT_RETRIES = 99
    RETRY_DELAY_S = 10
    
    DEPOS_MAP = {
        "DEP": "BBLANCADE", "MDQ": "MARDELGUEM", "MONBAHIA": "BBLANCA",
        "MTGBBPS": "BBLANCA", "MTGCBA": "CORDOBA", "MTGCOM": "NEUQUENCOMAHUE",
        "MTGJBJ": "MARDELJUAN", "MTGROCA": "RIONEGROMT", "MUNDOAL": "BBLANCAMUN",
        "MUNDOCAB": "PALERMO", "MUNDOROC": "RIONEGROMD", "NQNALB": "NEUQUENCENTRO",
        "NQNSHOP": "NEUQUENANONIMA",
    }
    
    API_BLOCK_RE = re.compile(r"\[API:[^\]]*(?:\]|$)", re.I)
    TRAIL_RE = re.compile(r"(?:\b[A-Z0-9]{2,}|NO)(?:,\s*\d+|\s+\d+)?\]\s*$")
    
    def __init__(self):
        self.puntos: Dict[str, float] = {}
        self.mult: Dict[str, float] = {}
        self._load_priorities()
    
    def _load_priorities(self) -> None:
        """Carga prioridades de depósitos desde datos integrados."""
        try:
            # Cargar prioridades integradas directamente
            self.puntos = self.PRIORIDADES_PUNTOS.copy()
            self.mult = self.PRIORIDADES_MULT.copy()
            
            log.info("✅ Prioridades integradas cargadas: %d puntos, %d multiplicadores", 
                    len(self.puntos), len(self.mult))
        except Exception as e:
            log.error("❌ Error cargando prioridades integradas: %s", e)
            # Fallback a valores por defecto
            self.puntos = {"DEP": 10000000}
            self.mult = {"DEP": 1.0}
    
    def _val_for(self, dep: str, tab: Dict[str, float], default: float = 0.0) -> float:
        """Obtiene valor de prioridad para depósito."""
        return tab.get(dep.upper(), tab.get(dep[:3].upper(), default))
    
    def _refresh_token(self, old: dict) -> dict:
        """Refresca token de ML."""
        try:
            r = requests.post(self.TOKEN_URL, timeout=10, data={
                "grant_type": "refresh_token",
                "client_id": self.CLIENT_ID,
                "client_secret": self.CLIENT_SECRET,
                "refresh_token": old["refresh_token"]
            })
            r.raise_for_status()
            
            new = r.json()
            new["created_at"] = int(time.time())
            new.setdefault("refresh_token", old["refresh_token"])
            
            # Guardar token actualizado
            tmp = self.TOKEN_PATH.with_suffix(".tmp")
            tmp.write_text(json.dumps(new, indent=2), "utf-8")
            tmp.replace(self.TOKEN_PATH)
            
            log.info("Token ML refrescado exitosamente")
            return new
        except Exception as e:
            log.error("Error refrescando token ML: %s", e)
            raise
    
    def _get_token(self, force: bool = False) -> Tuple[str, str]:
        """Obtiene token ML válido."""
        try:
            if not self.TOKEN_PATH.exists():
                raise FileNotFoundError(f"Token file not found: {self.TOKEN_PATH}")
            
            t = json.load(self.TOKEN_PATH.open())
            if force or time.time() > t["created_at"] + t["expires_in"] - 120:
                t = self._refresh_token(t)
            
            return t["access_token"], str(t["user_id"])
        except Exception as e:
            log.error("Error obteniendo token ML: %s", e)
            raise
    
    def _api_get(self, url: str, tok: str, tries: int = 2):
        """Realiza GET request a API ML con retry en caso de 401."""
        for _ in range(tries):
            r = requests.get(url, headers={"Authorization": f"Bearer {tok}"}, timeout=10)
            if r.status_code != 401:
                r.raise_for_status()
                return r
            tok, _ = self._get_token(True)
        r.raise_for_status()
    
    def _sku_item(self, it: dict) -> str:
        """Extrae SKU de item ML."""
        return (it.get("seller_sku") or it.get("seller_custom_field") or
                it.get("item", {}).get("seller_sku") or 
                it.get("item", {}).get("seller_custom_field") or "").strip()
    
    def _stock_por_deposito(self, sku: str) -> Dict[str, int]:
        """Consulta stock por depósito desde SQL Server."""
        try:
            art, col, tal = sku.split("-", 2)
        except ValueError:
            log.warning("SKU inválido (debe tener formato ART-COL-TAL): %s", sku)
            return {}
        
        query = (
            "SELECT RTRIM(BDALTAFW), SUM(COCANT) FROM [{db}].[ZooLogic].[COMB] "
            "WHERE RTRIM(COART)=? AND RTRIM(COCOL)=? AND RTRIM(TALLE)=? "
            "GROUP BY BDALTAFW HAVING SUM(COCANT)<>0"
        )
        
        res = {}
        for intento in range(self.CONNECT_RETRIES):
            try:
                with pyodbc.connect(self.CONN_STR, autocommit=True) as con:
                    cur = con.cursor()
                    for (db,) in cur.execute(
                        "SELECT name FROM sys.databases WHERE name LIKE 'DRAGONFISH_%' AND state_desc='ONLINE'"
                    ).fetchall():
                        try:
                            for dep, qty in cur.execute(query.format(db=db), art, col, tal):
                                dep_clean = dep.strip().upper()
                                res[dep_clean] = res.get(dep_clean, 0) + int(qty)
                        except pyodbc.Error:
                            continue
                break
            except pyodbc.OperationalError:
                if intento + 1 == self.CONNECT_RETRIES:
                    raise
                time.sleep(self.RETRY_DELAY_S)
        
        # Filtrar bases excluidas
        return {d: q for d, q in res.items() if d not in self.BASES_EXCLUIDAS}
    
    def _combo_score(self, req: Dict[str, int], combo: Tuple[str], mat: Dict[str, Dict[str, int]]) -> float:
        """Calcula score de combinación de depósitos."""
        s = 0.0
        for sku, need in req.items():
            rem = need
            for dep in combo:
                q = mat.get(dep, {}).get(sku, 0)
                if q <= 0 or rem == 0:
                    continue
                take = min(q, rem)
                s += self._val_for(dep, self.puntos) + take * self._val_for(dep, self.mult, 1.0)
                rem -= take
        return s
    
    def _lista_ganadores(self, req: Dict[str, int], mat: Dict[str, Dict[str, int]]) -> list[Tuple[str, int]]:
        """Lista depósitos ganadores ordenados por score."""
        g = []
        for dep, stk in mat.items():
            score = self._combo_score(req, (dep,), mat)
            if score > 0:
                g.append((dep, sum(stk.values()), score))
        
        g.sort(key=lambda x: x[2], reverse=True)
        return [(d, q) for d, q, _ in g]
    
    def _sig_ganador(self, lista: list[Tuple[str, int]], usados: set[str]) -> Optional[Tuple[str, int]]:
        """Obtiene siguiente ganador no usado."""
        for dep, qty in lista:
            if dep not in usados:
                return dep, qty
        return None
    
    def _leer_nota(self, oid: int, tok: str) -> dict:
        """Lee nota de orden ML."""
        try:
            j = self._api_get(f"https://api.mercadolibre.com/orders/{oid}/notes", tok).json()
            if isinstance(j, list) and j and j[0].get("results"):
                return j[0]["results"][0]
            if isinstance(j, dict) and j.get("id"):
                return j
        except HTTPError as e:
            if e.response.status_code in (403, 404):
                return {"id": None, "note": ""}
            raise
        return {"id": None, "note": ""}
    
    def _upsert_replace_api(self, oid: int, tok: str, linea: str) -> bool:
        """
        • Limpia cualquier bloque automático previo ("[API: …]" completo o cortado)
        • Graba solo «linea». Devuelve True si Mercado Libre aceptó la nota.
        """
        try:
            nota = self._leer_nota(oid, tok)
            txt = nota.get("note") or nota.get("plain_text", "") or ""

            # 1) sacar bloques [API: …]
            txt_clean = self.API_BLOCK_RE.sub("", txt)
            # 2) sacar fragmentos "Cancelado … Nuevo …" que hayan quedado
            txt_clean = re.sub(r"Cancelado [^]]*?Nuevo:[^]]*", "", txt_clean, flags=re.I)
            # 3) sacar colitas tipo "DEP 99]"
            txt_clean = self.TRAIL_RE.sub("", txt_clean).strip()

            nuevo = (txt_clean + " " + linea).strip() if txt_clean else linea
            if len(nuevo) > self.MAX_LEN:
                nuevo = nuevo[:self.MAX_LEN-3] + "..."

            payload = json.dumps({"note": nuevo})
            url = f"https://api.mercadolibre.com/orders/{oid}/notes"

            for _ in range(2):  # reintento en 401 / id inválido
                resp = (requests.put(f"{url}/{nota['id']}", 
                                   headers={"Authorization": f"Bearer {tok}"},
                                   data=payload, timeout=10)
                        if nota.get("id")
                        else requests.post(url, 
                                          headers={"Authorization": f"Bearer {tok}"},
                                          data=payload, timeout=10))
                
                if resp.status_code in (403, 404):
                    log.warning("Sin permisos para actualizar nota orden %s", oid)
                    return False
                    
                if resp.status_code == 400 and nota.get("id"):
                    nota["id"] = None
                    continue
                    
                if resp.status_code == 401:
                    tok, _ = self._get_token()
                    continue
                    
                resp.raise_for_status()
                log.info("Nota actualizada exitosamente para orden %s", oid)
                return True
                
        except Exception as e:
            log.error("Error actualizando nota orden %s: %s", oid, e)
            return False
    
    def _linea_cancel(self, nota_old: str, dep_cancel: str, motivo: str, 
                     dep_nuevo: str, cant_nueva: int) -> str:
        """Construye línea de cancelación para nota ML."""
        pasos = []
        m_hist = re.search(r"\[API:\s*Cancelado\s+(.+?)\.\s*Nuevo:", nota_old, re.I)
        if m_hist:
            pasos = [p.strip() for p in m_hist.group(1).split(",") if p.strip()]
        
        loc_cancel = self.DEPOS_MAP.get(dep_cancel, dep_cancel)
        nuevo = f"{len(pasos)+1}){loc_cancel}:{motivo}"
        
        if not any(p.split(')', 1)[1] == nuevo.split(')', 1)[1] for p in pasos):
            pasos.append(nuevo)
        
        # Construir línea final
        while True:
            cuerpo = ", ".join(pasos)
            linea = f'[API: Cancelado {cuerpo}. Nuevo: {dep_nuevo} {cant_nueva}]'
            if len(linea) <= self.MAX_LEN or not pasos:
                break
            pasos.pop(0)
        
        return linea[:self.MAX_LEN]
    
    def _get_customer_phone(self, order: dict, tok: str) -> Optional[str]:
        """Obtiene el teléfono del cliente desde la orden ML."""
        log.info("🔍 Buscando teléfono del cliente...")
        
        try:
            # 1. Intentar desde buyer info
            buyer_id = order.get("buyer", {}).get("id")
            log.info("👤 Buyer ID: %s", buyer_id)
            
            if buyer_id:
                try:
                    buyer_url = f"https://api.mercadolibre.com/users/{buyer_id}"
                    buyer_data = self._api_get(buyer_url, tok).json()
                    log.info("📞 Datos buyer obtenidos: %s", list(buyer_data.keys()))
                    
                    # Intentar phone.number
                    phone_data = buyer_data.get("phone", {})
                    if phone_data and isinstance(phone_data, dict):
                        phone = phone_data.get("number")
                        log.info("📱 Phone.number: %s", phone)
                        if phone:
                            phone = self._clean_phone(phone)
                            if phone:
                                return phone
                    
                    # Intentar alternative_phone
                    alt_phone = buyer_data.get("alternative_phone")
                    log.info("📲 Alternative phone: %s", alt_phone)
                    if alt_phone:
                        phone = self._clean_phone(alt_phone)
                        if phone:
                            return phone
                            
                except Exception as e:
                    log.warning("⚠️ Error obteniendo datos del buyer: %s", e)
            
            # 2. Intentar desde shipping
            shipping = order.get("shipping", {})
            log.info("🚚 Shipping data: %s", list(shipping.keys()) if shipping else "No shipping")
            
            if shipping:
                # receiver_phone
                receiver_phone = shipping.get("receiver_phone")
                log.info("📦 Receiver phone: %s", receiver_phone)
                if receiver_phone:
                    phone = self._clean_phone(receiver_phone)
                    if phone:
                        return phone
                
                # contact_phone desde receiver_address
                receiver_addr = shipping.get("receiver_address", {})
                contact_phone = receiver_addr.get("contact_phone")
                log.info("🏠 Contact phone: %s", contact_phone)
                if contact_phone:
                    phone = self._clean_phone(contact_phone)
                    if phone:
                        return phone
            
            # 3. Intentar desde payments (a veces tiene teléfono)
            payments = order.get("payments", [])
            log.info("💳 Payments: %d items", len(payments) if payments else 0)
            
            for payment in payments or []:
                payer = payment.get("payer", {})
                payer_phone = payer.get("phone", {})
                if isinstance(payer_phone, dict):
                    phone = payer_phone.get("number")
                    log.info("💴 Payer phone: %s", phone)
                    if phone:
                        phone = self._clean_phone(phone)
                        if phone:
                            return phone
                            
        except Exception as e:
            log.error("❌ Error general obteniendo teléfono: %s", e)
        
        log.warning("⚠️ No se encontró teléfono del cliente en ninguna fuente")
        return None
    
    def _clean_phone(self, phone: str) -> Optional[str]:
        """Limpia y valida un número de teléfono."""
        if not phone:
            return None
        
        log.info("🧹 Limpiando teléfono: '%s'", phone)
        
        # Limpiar: solo dígitos (remover +, espacios, guiones, paréntesis)
        phone_clean = ''.join(filter(str.isdigit, str(phone)))
        log.info("🔢 Solo dígitos: '%s'", phone_clean)
        
        # Casos comunes en Argentina:
        # +54 9 2915 16-3952 -> 5492915163952
        # 54 9 2915 163952 -> 5492915163952  
        # 2915 163952 -> 2915163952
        
        if phone_clean.startswith('54'):
            # Formato internacional: 54 + código área + número
            if len(phone_clean) >= 12:  # 54 + 10 dígitos mínimo
                # Remover 54 del inicio
                local_phone = phone_clean[2:]
                log.info("🇦🇷 Formato internacional, teléfono local: %s", local_phone)
                
                # Si empieza con 9 (celular), mantenerlo
                if local_phone.startswith('9') and len(local_phone) >= 10:
                    log.info("✅ Teléfono celular válido: %s****", local_phone[:6])
                    return local_phone
                # Si no empieza con 9 pero tiene 10+ dígitos, también válido
                elif len(local_phone) >= 10:
                    log.info("✅ Teléfono fijo válido: %s****", local_phone[:6])
                    return local_phone
        
        elif phone_clean.startswith('9') and len(phone_clean) >= 10:
            # Formato nacional celular: 9 + código área + número
            log.info("✅ Teléfono celular nacional válido: %s****", phone_clean[:6])
            return phone_clean
            
        elif len(phone_clean) >= 10:
            # Formato local: código área + número (sin 54 ni 9)
            log.info("✅ Teléfono local válido: %s****", phone_clean[:6])
            return phone_clean
        
        log.warning("❌ Teléfono inválido o muy corto: '%s' (longitud: %d)", phone_clean, len(phone_clean))
        return None
    
    def _open_whatsapp(self, phone: str, order_id: int, reason: str, new_depot: str) -> None:
        """Abre WhatsApp con mensaje predefinido para el cliente."""
        try:
            import webbrowser
            import urllib.parse
            
            log.info("🔧 Formateando teléfono: %s", phone)
            
            # Formatear teléfono para WhatsApp (agregar código país Argentina)
            if not phone.startswith('549'):
                if phone.startswith('9'):
                    phone = '54' + phone
                else:
                    phone = '549' + phone
            
            log.info("🇦🇷 Teléfono formateado: %s", phone)
            
            # Mensaje predefinido
            message = (
                f"Hola! Te contacto por tu compra #{order_id}. "
                f"Lamentablemente tuvimos que cancelar el artículo por: {reason}. "
                f"Ya reasignamos tu pedido al depósito {new_depot}. "
                f"¡Gracias por tu paciencia!"
            )
            
            log.info("💬 Mensaje: %s", message[:50] + "...")
            
            # URL de WhatsApp con encoding correcto
            encoded_message = urllib.parse.quote(message)
            whatsapp_url = f"https://wa.me/{phone}?text={encoded_message}"
            
            log.info("🔗 URL WhatsApp: %s", whatsapp_url[:80] + "...")
            
            # Abrir en navegador con más control
            success = webbrowser.open(whatsapp_url)
            
            if success:
                log.info("✅ WhatsApp abierto exitosamente para cliente %s (orden %s)", phone[-4:], order_id)
            else:
                log.warning("⚠️ webbrowser.open() retornó False")
                # Intentar con navegador específico
                import subprocess
                subprocess.run(["start", whatsapp_url], shell=True, check=False)
                log.info("🔄 Intento alternativo con subprocess")
            
        except Exception as e:
            log.error("❌ Error abriendo WhatsApp: %s", e)
    
    def _linea_cancel(self, nota_old: str, dep_cancel: str, motivo: str, 
                     dep_nuevo: str, cant_nueva: int) -> str:
        """Genera línea de cancelación con mapeo de depósitos."""
        pasos = []
        
        # Buscar historial de cancelaciones previas
        m_hist = re.search(r"\[API:\s*Cancelado\s+(.+?)\.\s*Nuevo:", nota_old, re.I)
        if m_hist:
            pasos = [p.strip() for p in m_hist.group(1).split(",") if p.strip()]
        
        # Mapear depósito cancelado a nombre legible
        loc_cancel = self.DEPOS_MAP.get(dep_cancel.upper(), dep_cancel)
        nuevo = f"{len(pasos)+1}){loc_cancel}:{motivo}"
        
        # Evitar duplicados
        if not any(p.split(')',1)[1]==nuevo.split(')',1)[1] for p in pasos):
            pasos.append(nuevo)
        
        # Generar línea final respetando límite de caracteres
        MAX_LEN = 240
        while True:
            cuerpo = ", ".join(pasos)
            linea = f'[API: Cancelado {cuerpo}. Nuevo: {dep_nuevo} {cant_nueva}]'
            if len(linea) <= MAX_LEN or not pasos:
                break
            pasos.pop(0)  # Remover paso más antiguo si es muy largo
        
        return linea[:MAX_LEN]
            # No hacer raise para que la cancelación continúe
    
    def cancel_order(self, order_id: int, reason: str) -> Tuple[bool, str]:
        """
        Cancela orden y recalcula depósito ganador (basado en server_daemon.py).
        
        Args:
            order_id: ID de la orden ML
            reason: Motivo de cancelación
            
        Returns:
            Tuple[bool, str]: (éxito, mensaje/nota_nueva)
        """
        try:
            log.info("Iniciando cancelación orden %s, motivo: %s", order_id, reason)
            
            tok, seller = self._get_token()
            oid = order_id  # Mantener referencia original

            # ─── localizar orden ───────────────────────────────────
            original_oid = oid  # Guardar ID original para la nota
            try:
                order = self._api_get(f"https://api.mercadolibre.com/orders/{oid}", tok).json()
                log.info("✅ Orden encontrada directamente: %s", oid)
            except HTTPError as e:
                if e.response.status_code == 404:
                    log.info("🔍 Orden no encontrada directamente, buscando como pack_id: %s", oid)
                    # Usar endpoint oficial de packs según documentación
                    pack_url = f"https://api.mercadolibre.com/packs/{oid}"
                    try:
                        pack_data = self._api_get(pack_url, tok).json()
                        orders_in_pack = pack_data.get("orders", [])
                        if not orders_in_pack:
                            return False, "Pack no contiene órdenes"
                        
                        # Tomar la primera orden del pack
                        first_order_id = orders_in_pack[0]["id"]
                        log.info("📦 Pack encontrado con %d órdenes. Usando orden: %s", len(orders_in_pack), first_order_id)
                        
                        # Obtener detalles de la primera orden
                        order = self._api_get(f"https://api.mercadolibre.com/orders/{first_order_id}", tok).json()
                        real_order_id = order["id"]
                        order_date = order.get("date_created", "")
                        
                    except HTTPError as pack_error:
                        if pack_error.response.status_code == 404:
                            return False, f"Pack {oid} no encontrado"
                        else:
                            return False, f"Error al consultar pack: {pack_error.response.text}"
                    
                    log.info("📦 Pack encontrado. Pack_id: %s, Real order_id: %s, Fecha: %s", 
                            oid, real_order_id, order_date[:10])
                    
                    # Verificar que la orden encontrada sea reciente (no de hace un año)
                    from datetime import datetime, timedelta
                    try:
                        order_datetime = datetime.fromisoformat(order_date.replace('Z', '+00:00'))
                        days_ago = (datetime.now(order_datetime.tzinfo) - order_datetime).days
                        
                        if days_ago > 30:
                            log.warning("⚠️ Orden encontrada es muy antigua (%d días). Error de ML API - usando ID original.", days_ago)
                            # FORZAR uso del ID original cuando ML API devuelve orden incorrecta
                            log.info("🔧 Forzando uso de ID original %s (ignorando orden antigua %s)", original_oid, real_order_id)
                            # Intentar obtener la orden original directamente para validar
                            try:
                                order = self._api_get(f"https://api.mercadolibre.com/orders/{original_oid}", tok).json()
                                log.info("✅ Orden original %s encontrada - procediendo con cancelación", original_oid)
                                # NO cambiar oid - usar original_oid para todo
                            except HTTPError as e2:
                                if e2.response.status_code == 404:
                                    return False, f"ID {original_oid} no existe como orden individual"
                                else:
                                    return False, f"Error al validar orden original: {e2.response.text}"
                        else:
                            log.info("✅ Orden válida: %d días de antigüedad", days_ago)
                            # Usar la orden real del pack (como en el código que funciona)
                            oid = real_order_id
                            log.info("🎯 Usando orden real del pack %s para actualizar nota", oid)
                        
                    except Exception as date_error:
                        log.warning("⚠️ No se pudo verificar fecha de orden: %s", date_error)
                        # En caso de error de fecha, usar la orden del pack
                        oid = real_order_id
                        log.info("🎯 Usando orden real del pack %s para actualizar nota", oid)
                else:
                    return False, f"Error al buscar orden: {e.response.text}"

            # ─── multiventa → salir informando ─────────────────────
            if len(order["order_items"]) > 1:
                return False, "Multiventa: cancelar manualmente en ML"

            # ─── requerimientos de stock por SKU ───────────────────
            req = defaultdict(int)
            for it in order["order_items"]:
                sku = self._sku_item(it)
                if sku.count("-") == 2:
                    req[sku] += it.get("quantity", 1)

            # ─── matriz de stock actual ────────────────────────────
            mat = defaultdict(dict)
            for sku in req:
                for dep, q in self._stock_por_deposito(sku).items():
                    if q > 0:
                        mat[dep][sku] = q

            candidatos = self._lista_ganadores(req, mat)
            if not candidatos:
                # ——— CASO 1: no hay stock en ningún depósito ———
                fecha = datetime.now().strftime("%d/%m")
                linea = f"[API: Cancelado Sin stock en otros depósitos · {fecha}]"
                self._upsert_replace_api(oid, tok, linea)
                return True, linea

            # ─── elegir próximo depósito ───────────────────────────
            nota_old = self._leer_nota(oid, tok).get("note", "")
            deps_usados = set(re.findall(r"\b([A-Z0-9]{2,})\b", nota_old))
            proximo = self._sig_ganador(candidatos, deps_usados)
            if not proximo:
                # ya usamos todos: deja nota fija
                fecha = datetime.now().strftime("%d/%m")
                linea = f"[API: Cancelado Sin stock en otros depósitos · {fecha}]"
                self._upsert_replace_api(oid, tok, linea)
                return True, linea

            dep_nuevo, cant_nueva = proximo

            # ─── depósito cancelado anterior (si existe) ───────────
            m_cancel = None
            for m in re.finditer(r"Nuevo:\s+([A-Z0-9]{2,})\b", nota_old):
                m_cancel = m
            dep_cancel = m_cancel.group(1) if m_cancel else candidatos[0][0]

            linea = self._linea_cancel(nota_old, dep_cancel, reason, dep_nuevo, cant_nueva)

            if not self._upsert_replace_api(oid, tok, linea):
                return False, "ML no permitió grabar la nota"

            log.info("✅ Nota actualizada en %s: %s", oid, linea)
            return True, linea

        except Exception as e:
            log.error("Error inesperado en cancelación %s: %s", order_id, e)
            return False, f"Error inesperado: {str(e)}"
    
    def cancel_item(self, order_id: str, sku: str, reason: str) -> Tuple[bool, str]:
        """
        Cancela un item específico de una orden (wrapper para cancel_order).
        
        Args:
            order_id: ID de la orden o pack_id
            sku: SKU del artículo (no usado en la implementación actual)
            reason: Motivo de cancelación
            
        Returns:
            Tuple[bool, str]: (éxito, mensaje)
        """
        try:
            # Convertir order_id a int si es string
            order_id_int = int(order_id)
            return self.cancel_order(order_id_int, reason)
        except ValueError:
            log.error("Error: order_id '%s' no es un número válido", order_id)
            return False, f"ID de orden inválido: {order_id}"
        except Exception as e:
            log.error("Error en cancel_item: %s", e)
            return False, f"Error cancelando artículo: {str(e)}"


# Instancia global del servicio
cancellation_service = CancellationService()
