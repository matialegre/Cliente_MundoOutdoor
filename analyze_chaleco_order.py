#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Script para analizar la orden específica del chaleco Weis
y verificar el seller_custom_field
"""

import requests
import json
from datetime import datetime, timedelta
from api import ml_api

def analyze_chaleco_order():
    """Analiza la orden específica del chaleco Weis."""
    print("🎒 ANÁLISIS DE ORDEN CHALECO WEIS")
    print("=" * 50)
    
    # Obtener token
    try:
        access_token, seller_id = ml_api.refresh_access_token()
        print(f"✅ Token obtenido para seller: {seller_id}")
    except Exception as e:
        print(f"❌ Error obteniendo token: {e}")
        return
    
    # Datos de la orden del usuario
    order_id = "2000008589094373"
    expected_sku = "101-W350-WML"
    
    print(f"\n🎯 ANALIZANDO ORDEN: {order_id}")
    print(f"📅 Fecha: 22 jul 11:02 hs")
    print(f"🎒 Producto: Chaleco Weis De Hidratacion 2+5l 240grs")
    print(f"👤 Cliente: Ayelen Tessicino")
    print(f"📍 Destino: Lincoln, Buenos Aires")
    print(f"📝 Nota: [API: 1) DEPOSITO 1] [STOCK -1]")
    print(f"🏷️ SKU mostrado: {expected_sku}")
    print(f"📏 Talle: M/L")
    
    # Buscar la orden por fecha y SKU
    date_target = datetime(2025, 7, 22, 11, 2)  # 22 jul 11:02 hs
    date_from = date_target.date()
    date_to = date_target.date()
    
    print(f"\n📅 Buscando en fecha: {date_target.date()}")
    
    try:
        # Usar la misma función que usa el picker
        raw_orders = ml_api.list_orders(seller_id, access_token, date_from, date_to)
        print(f"📦 Encontradas {len(raw_orders)} órdenes en {date_target.date()}")
        
        # Buscar la orden específica por ID
        target_order = None
        for order in raw_orders:
            if str(order.get('id')) == order_id:
                target_order = order
                print(f"🎯 ¡ORDEN ENCONTRADA POR ID!")
                break
        
        if not target_order:
            # Buscar por SKU si no se encuentra por ID
            print(f"🔍 Orden {order_id} no encontrada por ID, buscando por SKU: {expected_sku}")
            for order in raw_orders:
                order_items = order.get('order_items', [])
                for item in order_items:
                    item_sku = item.get('item', {}).get('seller_sku') or ''
                    if expected_sku in item_sku:
                        print(f"🎯 ¡ORDEN ENCONTRADA POR SKU!")
                        print(f"  Order ID real: {order.get('id')}")
                        target_order = order
                        break
                if target_order:
                    break
        
        if target_order:
            analyze_order_details(target_order, access_token)
        else:
            print(f"❌ No se encontró la orden")
            
            # Mostrar algunas órdenes del día para debug
            print(f"\n🔍 Órdenes encontradas en {date_target.date()}:")
            count = 0
            for order in raw_orders:
                order_items = order.get('order_items', [])
                for item in order_items:
                    item_sku = item.get('item', {}).get('seller_sku') or ''
                    item_title = item.get('item', {}).get('title', '')
                    if item_sku:
                        print(f"  - Orden: {order.get('id')}")
                        print(f"    SKU: {item_sku}")
                        print(f"    Producto: {item_title[:50]}...")
                        count += 1
                        if count >= 10:
                            break
                if count >= 10:
                    break
            
    except Exception as e:
        print(f"❌ Error buscando orden: {e}")
        import traceback
        traceback.print_exc()

def analyze_order_details(order, access_token):
    """Analiza los detalles completos de la orden encontrada."""
    print(f"\n📋 ANÁLISIS COMPLETO DE LA ORDEN:")
    
    order_id = order.get('id')
    print(f"  Order ID: {order_id}")
    print(f"  Status: {order.get('status')}")
    print(f"  Date Created: {order.get('date_created')}")
    print(f"  Total Amount: {order.get('total_amount')}")
    
    # Obtener nota de la orden
    try:
        note = ml_api.get_order_note(order_id, access_token)
        print(f"  Nota: {note}")
        
        # Verificar si la nota contiene keywords válidos
        KEYWORDS_NOTE = ['DEPO', 'MUNDOAL', 'MTGBBL', 'BBPS', 'MONBAHIA', 'MTGBBPS']
        note_up = (note or '').upper()
        has_keywords = any(keyword in note_up for keyword in KEYWORDS_NOTE)
        print(f"  ✅ Nota contiene keywords válidos: {has_keywords}")
        if has_keywords:
            matching_keywords = [k for k in KEYWORDS_NOTE if k in note_up]
            print(f"  📝 Keywords encontrados: {matching_keywords}")
        
    except Exception as e:
        print(f"  Nota: Error obteniendo - {e}")
    
    # Analizar items
    order_items = order.get('order_items', [])
    print(f"\n📦 ITEMS DE LA ORDEN ({len(order_items)}):")
    
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
        print(f"    Quantity: {item.get('quantity')}")
        
        # Analizar el item completo para obtener seller_custom_field
        if item_id:
            analyze_item_complete(item_id, variation_id, seller_sku, access_token)

def analyze_item_complete(item_id, variation_id, current_sku, access_token):
    """Analiza el item completo para obtener seller_custom_field."""
    print(f"\n    🔬 ANÁLISIS DETALLADO DEL ITEM:")
    
    try:
        url = f"https://api.mercadolibre.com/items/{item_id}"
        headers = {"Authorization": f"Bearer {access_token}"}
        
        resp = requests.get(url, headers=headers, timeout=10)
        resp.raise_for_status()
        item_data = resp.json()
        
        # Seller custom field del item principal
        main_custom_field = item_data.get('seller_custom_field')
        print(f"    📝 Seller Custom Field (item): {main_custom_field}")
        
        # Analizar variaciones
        variations = item_data.get('variations', [])
        if variations:
            print(f"    🎨 Variaciones encontradas: {len(variations)}")
            
            # Mostrar todas las variaciones para entender el patrón
            for i, var in enumerate(variations):
                var_id = var.get('id')
                var_custom_field = var.get('seller_custom_field')
                var_attrs = var.get('attribute_combinations', [])
                
                print(f"    📦 VARIACIÓN {i+1}:")
                print(f"      Variation ID: {var_id}")
                print(f"      Seller Custom Field: {var_custom_field}")
                
                # Mostrar atributos
                print(f"      Atributos:")
                for attr in var_attrs:
                    attr_id = attr.get('id')
                    attr_name = attr.get('name')
                    attr_value = attr.get('value_name') or attr.get('value_id')
                    print(f"        {attr_id}: {attr_name} = {attr_value}")
                
                # Marcar si es la variación de la orden
                if str(var_id) == str(variation_id):
                    print(f"      ⭐ ESTA ES LA VARIACIÓN DE LA ORDEN")
                    
                    # Comparar SKUs
                    print(f"\n    🧩 COMPARACIÓN DE SKUs:")
                    print(f"      SKU mostrado en orden: {current_sku}")
                    print(f"      Seller Custom Field: {var_custom_field}")
                    
                    if current_sku == var_custom_field:
                        print(f"      ✅ ¡COINCIDEN! El SKU ya es correcto")
                    else:
                        print(f"      ⚠️ DIFERENTES:")
                        print(f"        - Orden muestra: {current_sku}")
                        print(f"        - Campo real: {var_custom_field}")
                        if var_custom_field:
                            print(f"        💡 El SKU REAL sería: {var_custom_field}")
                
                print()  # Línea en blanco entre variaciones
        else:
            print(f"    📝 Item sin variaciones")
            if main_custom_field:
                print(f"    🧩 COMPARACIÓN DE SKUs:")
                print(f"      SKU mostrado: {current_sku}")
                print(f"      Seller Custom Field: {main_custom_field}")
                
                if current_sku == main_custom_field:
                    print(f"      ✅ ¡COINCIDEN! El SKU ya es correcto")
                else:
                    print(f"      ⚠️ DIFERENTES - El SKU real sería: {main_custom_field}")
        
    except Exception as e:
        print(f"    ❌ Error analizando item: {e}")
        import traceback
        traceback.print_exc()

def main():
    """Función principal."""
    analyze_chaleco_order()
    print(f"\n✅ Análisis completado")

if __name__ == "__main__":
    main()
