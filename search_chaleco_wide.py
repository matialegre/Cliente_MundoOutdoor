#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Búsqueda amplia del chaleco Weis en múltiples días
"""

import requests
import json
from datetime import datetime, timedelta
from api import ml_api

def search_chaleco_wide():
    """Busca el chaleco en un rango amplio de fechas."""
    print("🎒 BÚSQUEDA AMPLIA - CHALECO WEIS")
    print("=" * 50)
    
    # Obtener token
    try:
        access_token, seller_id = ml_api.refresh_access_token()
        print(f"✅ Token obtenido para seller: {seller_id}")
    except Exception as e:
        print(f"❌ Error obteniendo token: {e}")
        return
    
    # Datos a buscar
    target_sku = "101-W350-WML"
    target_product = "Chaleco Weis De Hidratacion"
    
    print(f"\n🔍 BUSCANDO:")
    print(f"🏷️ SKU: {target_sku}")
    print(f"🎒 Producto: {target_product}")
    
    # Buscar en los últimos 5 días
    today = datetime.now().date()
    
    for days_back in range(5):
        search_date = today - timedelta(days=days_back)
        print(f"\n📅 Buscando en: {search_date}")
        
        try:
            raw_orders = ml_api.list_orders(seller_id, access_token, search_date, search_date)
            print(f"📦 {len(raw_orders)} órdenes encontradas")
            
            # Buscar por SKU
            for order in raw_orders:
                order_items = order.get('order_items', [])
                for item in order_items:
                    item_sku = item.get('item', {}).get('seller_sku') or ''
                    item_title = item.get('item', {}).get('title', '')
                    
                    # Buscar por SKU exacto
                    if target_sku in item_sku:
                        print(f"🎯 ¡ENCONTRADO POR SKU!")
                        analyze_found_order(order, access_token)
                        return
                    
                    # Buscar por nombre del producto
                    if "chaleco" in item_title.lower() and "weis" in item_title.lower():
                        print(f"🎯 ¡ENCONTRADO POR NOMBRE!")
                        print(f"  Orden: {order.get('id')}")
                        print(f"  SKU: {item_sku}")
                        print(f"  Título: {item_title}")
                        analyze_found_order(order, access_token)
                        return
            
        except Exception as e:
            print(f"❌ Error buscando en {search_date}: {e}")
    
    print(f"\n❌ No se encontró el chaleco en los últimos 5 días")
    
    # Mostrar algunos productos similares encontrados
    print(f"\n🔍 Productos similares encontrados:")
    today = datetime.now().date()
    
    try:
        raw_orders = ml_api.list_orders(seller_id, access_token, today, today)
        count = 0
        for order in raw_orders:
            order_items = order.get('order_items', [])
            for item in order_items:
                item_sku = item.get('item', {}).get('seller_sku') or ''
                item_title = item.get('item', {}).get('title', '')
                
                # Buscar productos con "101" o "W350"
                if "101" in item_sku or "W350" in item_sku or "chaleco" in item_title.lower():
                    print(f"  - SKU: {item_sku}")
                    print(f"    Producto: {item_title[:60]}...")
                    print(f"    Orden: {order.get('id')}")
                    count += 1
                    if count >= 5:
                        break
            if count >= 5:
                break
    except:
        pass

def analyze_found_order(order, access_token):
    """Analiza la orden encontrada."""
    print(f"\n📋 ANÁLISIS DE LA ORDEN ENCONTRADA:")
    
    order_id = order.get('id')
    print(f"  Order ID: {order_id}")
    print(f"  Status: {order.get('status')}")
    print(f"  Date Created: {order.get('date_created')}")
    
    # Obtener nota
    try:
        note = ml_api.get_order_note(order_id, access_token)
        print(f"  Nota: {note}")
    except:
        print(f"  Nota: No disponible")
    
    # Analizar items
    order_items = order.get('order_items', [])
    for i, item in enumerate(order_items):
        print(f"\n  ITEM {i+1}:")
        item_data = item.get('item', {})
        item_id = item_data.get('id')
        variation_id = item_data.get('variation_id')
        seller_sku = item_data.get('seller_sku')
        title = item_data.get('title')
        
        print(f"    Item ID: {item_id}")
        print(f"    Variation ID: {variation_id}")
        print(f"    Title: {title}")
        print(f"    Seller SKU: {seller_sku}")
        
        # Analizar seller_custom_field
        if item_id:
            analyze_custom_field(item_id, variation_id, seller_sku, access_token)

def analyze_custom_field(item_id, variation_id, current_sku, access_token):
    """Analiza el seller_custom_field del item."""
    print(f"\n    🔬 SELLER CUSTOM FIELD:")
    
    try:
        url = f"https://api.mercadolibre.com/items/{item_id}"
        headers = {"Authorization": f"Bearer {access_token}"}
        
        resp = requests.get(url, headers=headers, timeout=10)
        resp.raise_for_status()
        item_data = resp.json()
        
        # Campo del item principal
        main_custom_field = item_data.get('seller_custom_field')
        print(f"    📝 Item Custom Field: {main_custom_field}")
        
        # Variaciones
        variations = item_data.get('variations', [])
        if variations:
            print(f"    🎨 {len(variations)} variaciones encontradas")
            
            for var in variations:
                var_id = var.get('id')
                var_custom_field = var.get('seller_custom_field')
                
                if str(var_id) == str(variation_id):
                    print(f"    ⭐ VARIACIÓN DE LA ORDEN:")
                    print(f"      Custom Field: {var_custom_field}")
                    
                    # Comparar
                    print(f"\n    🧩 COMPARACIÓN:")
                    print(f"      SKU mostrado: {current_sku}")
                    print(f"      Custom Field: {var_custom_field}")
                    
                    if current_sku == var_custom_field:
                        print(f"      ✅ ¡COINCIDEN!")
                    else:
                        print(f"      ⚠️ DIFERENTES")
                        if var_custom_field:
                            print(f"      💡 SKU real sería: {var_custom_field}")
                    break
        else:
            print(f"    📝 Sin variaciones")
            if main_custom_field:
                print(f"    🧩 COMPARACIÓN:")
                print(f"      SKU mostrado: {current_sku}")
                print(f"      Custom Field: {main_custom_field}")
                
                if current_sku == main_custom_field:
                    print(f"      ✅ ¡COINCIDEN!")
                else:
                    print(f"      ⚠️ DIFERENTES - SKU real: {main_custom_field}")
        
    except Exception as e:
        print(f"    ❌ Error: {e}")

if __name__ == "__main__":
    search_chaleco_wide()
