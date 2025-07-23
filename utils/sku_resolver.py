#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Utilidad para resolver SKUs reales de productos con sufijo OUT
especialmente para zapatillas que tienen el SKU real en seller_custom_field
"""

import requests
import logging
from typing import Optional, Dict, Any
from functools import lru_cache

log = logging.getLogger(__name__)

class SKUResolver:
    """Resuelve SKUs reales para productos con sufijo OUT."""
    
    def __init__(self, access_token: str):
        self.access_token = access_token
        
    @lru_cache(maxsize=500)
    def get_real_sku(self, item_id: str, variation_id: Optional[str], current_sku: str) -> str:
        """
        Obtiene el SKU real para un producto.
        
        Args:
            item_id: ID del item en MercadoLibre
            variation_id: ID de la variación (puede ser None)
            current_sku: SKU actual que se muestra
            
        Returns:
            SKU real si se puede obtener, sino el SKU actual
        """
        # Si no termina en OUT, devolver el SKU actual
        if not current_sku or not current_sku.endswith('OUT'):
            return current_sku
            
        log.debug(f"🔍 Resolviendo SKU real para: {current_sku} (item: {item_id}, var: {variation_id})")
        
        try:
            # Obtener detalles del item
            item_data = self._get_item_details(item_id)
            if not item_data:
                log.warning(f"⚠️ No se pudo obtener detalles del item: {item_id}")
                return current_sku
            
            # Si tiene variaciones, buscar en la variación específica
            variations = item_data.get('variations', [])
            if variations and variation_id:
                real_sku = self._get_sku_from_variation(variations, variation_id)
                if real_sku:
                    log.info(f"✅ SKU real encontrado: {current_sku} → {real_sku}")
                    return real_sku
            
            # Si no hay variaciones o no se encontró, usar seller_custom_field del item
            item_custom_field = item_data.get('seller_custom_field')
            if item_custom_field and item_custom_field != current_sku:
                log.info(f"✅ SKU real del item: {current_sku} → {item_custom_field}")
                return item_custom_field
            
            log.debug(f"🤷 No se encontró SKU alternativo para: {current_sku}")
            return current_sku
            
        except Exception as e:
            log.error(f"❌ Error resolviendo SKU para {current_sku}: {e}")
            return current_sku
    
    def _get_item_details(self, item_id: str) -> Optional[Dict[str, Any]]:
        """Obtiene los detalles completos de un item."""
        url = f"https://api.mercadolibre.com/items/{item_id}"
        headers = {"Authorization": f"Bearer {self.access_token}"}
        
        try:
            resp = requests.get(url, headers=headers, timeout=10)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            log.error(f"❌ Error obteniendo item {item_id}: {e}")
            return None
    
    def _get_sku_from_variation(self, variations: list, variation_id: str) -> Optional[str]:
        """Busca el SKU real en una variación específica."""
        try:
            variation_id_int = int(variation_id)
            for var in variations:
                if var.get('id') == variation_id_int:
                    custom_field = var.get('seller_custom_field')
                    if custom_field:
                        log.debug(f"📝 Variación {variation_id}: seller_custom_field = {custom_field}")
                        return custom_field
                    break
        except (ValueError, TypeError):
            log.warning(f"⚠️ ID de variación inválido: {variation_id}")
        
        return None
    
    def resolve_multiple_skus(self, items_data: list) -> Dict[str, str]:
        """
        Resuelve múltiples SKUs de una vez.
        
        Args:
            items_data: Lista de diccionarios con keys: item_id, variation_id, sku
            
        Returns:
            Diccionario {sku_original: sku_real}
        """
        resolved = {}
        
        for item_data in items_data:
            item_id = item_data.get('item_id')
            variation_id = item_data.get('variation_id')
            current_sku = item_data.get('sku', '')
            
            if item_id and current_sku:
                real_sku = self.get_real_sku(item_id, variation_id, current_sku)
                resolved[current_sku] = real_sku
        
        return resolved
    
    def clear_cache(self):
        """Limpia el cache de SKUs resueltos."""
        self.get_real_sku.cache_clear()
        log.info("🧹 Cache de SKUs limpiado")


# Función utilitaria global
def resolve_sku_if_needed(access_token: str, item_id: str, variation_id: Optional[str], current_sku: str) -> str:
    """
    Función utilitaria para resolver un SKU si es necesario.
    
    Args:
        access_token: Token de acceso a ML
        item_id: ID del item
        variation_id: ID de la variación (opcional)
        current_sku: SKU actual
        
    Returns:
        SKU real o el actual si no se puede resolver
    """
    if not current_sku or not current_sku.endswith('OUT'):
        return current_sku
    
    resolver = SKUResolver(access_token)
    return resolver.get_real_sku(item_id, variation_id, current_sku)


def is_out_sku(sku: str) -> bool:
    """Verifica si un SKU tiene el sufijo OUT problemático."""
    return bool(sku and sku.endswith('OUT'))


def get_base_sku_from_out(sku: str) -> str:
    """Extrae el SKU base removiendo el sufijo OUT."""
    if is_out_sku(sku):
        return sku[:-3]  # Quitar 'OUT'
    return sku
