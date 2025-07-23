#!/usr/bin/env python3
"""
Script de prueba para investigar el seller_custom_field en productos de MercadoLibre.
Especialmente para zapatillas que terminan en "OUT" como NMIDKUDZDWOUT.
"""

import requests
import json
from utils import config
from api import ml_api

def get_access_token():
    """Obtiene el access token de ML usando la función del sistema."""
    try:
        access_token, seller_id = ml_api.refresh_access_token()
        print(f"✅ Token obtenido exitosamente para seller: {seller_id}")
        return access_token, seller_id
    except Exception as e:
        print(f"❌ Error obteniendo token: {e}")
        return None, None

def search_items_by_sku_pattern(access_token, seller_id, pattern="OUT"):
    """Busca items que contengan un patrón específico en el SKU."""
    url = f"https://api.mercadolibre.com/users/{seller_id}/items/search"
    headers = {"Authorization": f"Bearer {access_token}"}
    
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        
        data = response.json()
        items = data.get('results', [])
        
        print(f"🔍 Encontrados {len(items)} items totales")
        
        # Filtrar items que terminen en "OUT" o contengan el patrón
        matching_items = []
        for item_id in items[:20]:  # Solo los primeros 20 para prueba
            item_details = get_item_details(access_token, item_id)
            if item_details:
                sku = item_details.get('seller_custom_field', '')
                title = item_details.get('title', '')
                
                if pattern.upper() in sku.upper() or pattern.upper() in title.upper():
                    matching_items.append({
                        'item_id': item_id,
                        'title': title,
                        'seller_custom_field': sku,
                        'variations': item_details.get('variations', [])
                    })
        
        return matching_items
        
    except Exception as e:
        print(f"❌ Error buscando items: {e}")
        return []

def get_item_details(access_token, item_id):
    """Obtiene detalles completos de un item específico."""
    url = f"https://api.mercadolibre.com/items/{item_id}"
    headers = {"Authorization": f"Bearer {access_token}"}
    
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"❌ Error obteniendo detalles del item {item_id}: {e}")
        return None

def analyze_zapatillas_out(access_token, seller_id):
    """Analiza específicamente zapatillas que terminan en OUT."""
    print("🔍 Buscando zapatillas con patrón 'OUT'...")
    
    matching_items = search_items_by_sku_pattern(access_token, seller_id, "OUT")
    
    if not matching_items:
        print("❌ No se encontraron items con patrón 'OUT'")
        return
    
    print(f"\n✅ Encontrados {len(matching_items)} items con patrón 'OUT':")
    print("=" * 80)
    
    for item in matching_items:
        print(f"\n📦 ITEM ID: {item['item_id']}")
        print(f"📝 TÍTULO: {item['title']}")
        print(f"🏷️  SKU (seller_custom_field): {item['seller_custom_field']}")
        
        # Analizar variaciones si existen
        variations = item.get('variations', [])
        if variations:
            print(f"🔄 VARIACIONES ({len(variations)}):")
            for i, var in enumerate(variations[:5]):  # Solo las primeras 5
                var_id = var.get('id', 'N/A')
                var_attrs = var.get('attribute_combinations', [])
                var_sku = var.get('seller_custom_field', 'N/A')
                
                # Extraer talle y color de los atributos
                talle = color = "N/A"
                for attr in var_attrs:
                    if attr.get('id') == 'SIZE':
                        talle = attr.get('value_name', 'N/A')
                    elif attr.get('id') == 'COLOR':
                        color = attr.get('value_name', 'N/A')
                
                print(f"   └─ Var {i+1}: ID={var_id}, SKU={var_sku}, Talle={talle}, Color={color}")
        else:
            print("🔄 Sin variaciones")
        
        # Intentar detectar patrón para SKU correcto
        sku = item['seller_custom_field']
        if sku.endswith('OUT'):
            base_sku = sku[:-3]  # Quitar "OUT"
            if 'W' in base_sku:
                # Buscar hasta la última W
                w_pos = base_sku.rfind('W')
                proposed_base = base_sku[:w_pos+1]  # Incluir la W
                print(f"💡 PROPUESTA: Base SKU = '{proposed_base}' + código_color + talle")
        
        print("-" * 40)

def main():
    """Función principal de prueba."""
    print("🧪 SCRIPT DE PRUEBA - SELLER CUSTOM FIELD")
    print("=" * 50)
    
    access_token, seller_id = get_access_token()
    if not access_token or not seller_id:
        print("❌ No se pudo obtener el access token")
        return
    
    print(f"🔍 Analizando items del seller: {seller_id}")
    
    analyze_zapatillas_out(access_token, seller_id)
    
    print("\n✅ Análisis completado")

if __name__ == "__main__":
    main()
