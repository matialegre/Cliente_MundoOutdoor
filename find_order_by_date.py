#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Script para buscar una orden específica por fecha y analizar su SKU
usando el mismo método que el programa de picking
"""

import requests
import json
from datetime import datetime, timedelta
from api import ml_api

def search_order_by_date_and_sku(access_token, seller_id, target_date, target_sku):
    """Busca una orden específica por fecha y SKU usando el método del programa."""
    print(f"🔍 Buscando orden con SKU: {target_sku}")
    print(f"📅 Fecha objetivo: {target_date}")
    
    # Buscar en un rango de fechas alrededor del objetivo
    date_from = target_date - timedelta(days=1)
    date_to = target_date + timedelta(days=1)
    
    print(f"📅 Rango de búsqueda: {date_from.strftime('%Y-%m-%d')} a {date_to.strftime('%Y-%m-%d')}")
    
    try:
        # Usar la misma función que usa el picker
        raw_orders = ml_api.list_orders(seller_id, access_token, date_from, date_to)
        print(f"📦 Encontradas {len(raw_orders)} órdenes en el rango")
        
        # Buscar la orden con el SKU específico
        target_order = None
        for order in raw_orders:
            order_items = order.get('order_items', [])
            for item in order_items:
                item_sku = item.get('item', {}).get('seller_sku') or ''
                if item_sku and target_sku in item_sku:
                    print(f"🎯 ¡ORDEN ENCONTRADA!")
                    print(f"  Order ID: {order.get('id')}")
                    print(f"  Date: {order.get('date_created')}")
                    print(f"  Status: {order.get('status')}")
                    print(f"  SKU encontrado: {item_sku}")
                    target_order = order
                    break
            
            if target_order:
                break
        
        if target_order:
            return analyze_found_order(target_order, access_token)
        else:
            print(f"❌ No se encontró orden con SKU: {target_sku}")
            
            # Mostrar algunos SKUs encontrados para debug
            print(f"\n🔍 SKUs encontrados en el rango:")
            count = 0
            for order in raw_orders[:10]:  # Solo primeras 10
                order_items = order.get('order_items', [])
                for item in order_items:
                    item_sku = item.get('item', {}).get('seller_sku') or ''
                    if item_sku:
                        print(f"  - {item_sku}")
                        count += 1
                        if count >= 20:  # Máximo 20 ejemplos
                            break
                if count >= 20:
                    break
            
            return None
            
    except Exception as e:
        print(f"❌ Error buscando órdenes: {e}")
        return None

def analyze_found_order(order, access_token):
    """Analiza la orden encontrada y extrae todos los datos."""
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
    except:
        print(f"  Nota: No disponible")
    
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
    
    return order

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
            
            for var in variations:
                var_id = var.get('id')
                var_custom_field = var.get('seller_custom_field')
                var_attrs = var.get('attribute_combinations', [])
                
                if var_id == variation_id:
                    print(f"    ⭐ VARIACIÓN DE LA ORDEN:")
                    print(f"      Variation ID: {var_id}")
                    print(f"      Seller Custom Field: {var_custom_field}")
                    
                    # Analizar atributos de la variación
                    print(f"      Atributos:")
                    for attr in var_attrs:
                        attr_id = attr.get('id')
                        attr_name = attr.get('name')
                        attr_value = attr.get('value_name') or attr.get('value_id')
                        print(f"        {attr_id}: {attr_name} = {attr_value}")
                    
                    # Analizar patrón SKU
                    analyze_sku_pattern(current_sku, var_custom_field)
                    break
        else:
            print(f"    📝 Item sin variaciones")
            if main_custom_field:
                analyze_sku_pattern(current_sku, main_custom_field)
        
    except Exception as e:
        print(f"    ❌ Error analizando item: {e}")

def analyze_sku_pattern(sku_with_out, seller_custom_field):
    """Analiza el patrón del SKU para entender la estructura."""
    print(f"\n    🧩 ANÁLISIS DE PATRÓN SKU:")
    print(f"      SKU actual: {sku_with_out}")
    print(f"      Seller Custom Field: {seller_custom_field}")
    
    if sku_with_out and seller_custom_field:
        if sku_with_out.endswith('OUT'):
            base_sku = sku_with_out[:-3]  # Quitar 'OUT'
            print(f"      Base SKU (sin OUT): {base_sku}")
            
            if seller_custom_field and seller_custom_field.startswith(base_sku):
                suffix = seller_custom_field[len(base_sku):]
                print(f"      Sufijo en seller_custom_field: '{suffix}'")
                print(f"      🎯 PATRÓN DETECTADO: {base_sku} + '{suffix}'")
                print(f"      💡 SKU CORRECTO SERÍA: {seller_custom_field}")
            else:
                print(f"      ⚠️ No hay coincidencia directa")
                print(f"      💡 POSIBLE SKU CORRECTO: {seller_custom_field}")
        else:
            print(f"      ℹ️ SKU no termina en 'OUT'")

def main():
    """Función principal."""
    print("🔍 BÚSQUEDA DE ORDEN POR FECHA Y SKU")
    print("=" * 50)
    
    # Obtener token
    try:
        access_token, seller_id = ml_api.refresh_access_token()
        print(f"✅ Token obtenido para seller: {seller_id}")
    except Exception as e:
        print(f"❌ Error obteniendo token: {e}")
        return
    
    # Datos de la orden del usuario
    target_date = datetime(2025, 7, 21, 16, 41)  # 21 jul 16:41 hs
    target_sku = "NMIDKUDZDWOUT"
    
    print(f"\n🎯 BUSCANDO ORDEN:")
    print(f"📅 Fecha: 21 jul 16:41 hs")
    print(f"🏷️ SKU: {target_sku}")
    print(f"🛍️ Producto: Zapatilla Bota Montagne Mujer Dynamo Trekking")
    
    order = search_order_by_date_and_sku(access_token, seller_id, target_date, target_sku)
    
    if order:
        print(f"\n✅ Análisis completado exitosamente")
    else:
        print(f"\n❌ No se pudo encontrar la orden")

if __name__ == "__main__":
    main()
