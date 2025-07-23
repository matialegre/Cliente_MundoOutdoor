#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Script para analizar una orden específica de MercadoLibre
y extraer todos los datos incluyendo seller_custom_field
"""

import requests
import json
from api import ml_api

def get_order_full_details(order_id, access_token):
    """Obtiene todos los detalles de una orden específica."""
    print(f"🔍 Analizando orden: {order_id}")
    
    # 1. Obtener detalles básicos de la orden
    url = f"https://api.mercadolibre.com/orders/{order_id}"
    headers = {"Authorization": f"Bearer {access_token}"}
    
    try:
        resp = requests.get(url, headers=headers, timeout=10)
        resp.raise_for_status()
        order_data = resp.json()
        
        print("📋 DATOS DE LA ORDEN:")
        print(f"  Order ID: {order_data.get('id')}")
        print(f"  Status: {order_data.get('status')}")
        print(f"  Date Created: {order_data.get('date_created')}")
        
        # 2. Analizar items de la orden
        items = order_data.get('order_items', [])
        for i, item in enumerate(items):
            print(f"\n📦 ITEM {i+1}:")
            item_id = item.get('item', {}).get('id')
            variation_id = item.get('item', {}).get('variation_id')
            
            print(f"  Item ID: {item_id}")
            print(f"  Variation ID: {variation_id}")
            print(f"  Title: {item.get('item', {}).get('title')}")
            print(f"  SKU: {item.get('item', {}).get('seller_sku')}")
            print(f"  Quantity: {item.get('quantity')}")
            
            # 3. Obtener detalles completos del item
            if item_id:
                analyze_item_details(item_id, variation_id, access_token)
        
        return order_data
        
    except Exception as e:
        print(f"❌ Error obteniendo orden: {e}")
        return None

def analyze_item_details(item_id, variation_id, access_token):
    """Analiza los detalles completos de un item incluyendo seller_custom_field."""
    print(f"\n🔬 ANÁLISIS DETALLADO DEL ITEM: {item_id}")
    
    # Obtener detalles del item
    url = f"https://api.mercadolibre.com/items/{item_id}"
    headers = {"Authorization": f"Bearer {access_token}"}
    
    try:
        resp = requests.get(url, headers=headers, timeout=10)
        resp.raise_for_status()
        item_data = resp.json()
        
        print(f"  Title: {item_data.get('title')}")
        print(f"  Seller SKU: {item_data.get('seller_custom_field')}")
        print(f"  Category ID: {item_data.get('category_id')}")
        
        # Analizar atributos
        attributes = item_data.get('attributes', [])
        print(f"\n🏷️ ATRIBUTOS ({len(attributes)}):")
        for attr in attributes:
            attr_id = attr.get('id')
            attr_name = attr.get('name')
            attr_value = attr.get('value_name') or attr.get('value_id')
            print(f"  {attr_id}: {attr_name} = {attr_value}")
        
        # Analizar variaciones si existen
        variations = item_data.get('variations', [])
        if variations:
            print(f"\n🎨 VARIACIONES ({len(variations)}):")
            for var in variations:
                var_id = var.get('id')
                var_sku = var.get('seller_custom_field')
                var_attrs = var.get('attribute_combinations', [])
                
                print(f"  Variation ID: {var_id}")
                print(f"  Seller SKU: {var_sku}")
                
                if var_id == variation_id:
                    print(f"  ⭐ ESTA ES LA VARIACIÓN DE LA ORDEN")
                
                print(f"  Atributos:")
                for attr in var_attrs:
                    attr_id = attr.get('id')
                    attr_name = attr.get('name')
                    attr_value = attr.get('value_name') or attr.get('value_id')
                    print(f"    {attr_id}: {attr_name} = {attr_value}")
                print()
        else:
            print("\n📝 Item sin variaciones")
            
        return item_data
        
    except Exception as e:
        print(f"❌ Error obteniendo item: {e}")
        return None

def analyze_sku_pattern(sku_with_out, seller_custom_field):
    """Analiza el patrón del SKU para entender la estructura."""
    print(f"\n🧩 ANÁLISIS DE PATRÓN SKU:")
    print(f"  SKU con OUT: {sku_with_out}")
    print(f"  Seller Custom Field: {seller_custom_field}")
    
    if sku_with_out and seller_custom_field:
        # Buscar patrones comunes
        if sku_with_out.endswith('OUT'):
            base_sku = sku_with_out[:-3]  # Quitar 'OUT'
            print(f"  Base SKU (sin OUT): {base_sku}")
            
            if seller_custom_field.startswith(base_sku):
                suffix = seller_custom_field[len(base_sku):]
                print(f"  Sufijo en seller_custom_field: {suffix}")
                print(f"  🎯 PATRÓN DETECTADO: {base_sku} + {suffix}")
            else:
                print(f"  ⚠️ No hay coincidencia directa entre base y seller_custom_field")
        else:
            print(f"  ⚠️ SKU no termina en 'OUT'")

def main():
    """Función principal."""
    print("🔍 ANÁLISIS DE ORDEN ESPECÍFICA")
    print("=" * 50)
    
    # Obtener token
    try:
        access_token, seller_id = ml_api.refresh_access_token()
        print(f"✅ Token obtenido para seller: {seller_id}")
    except Exception as e:
        print(f"❌ Error obteniendo token: {e}")
        return
    
    # Analizar la orden específica del usuario
    order_id = "2000008580539201"
    
    print(f"\n🎯 ANALIZANDO ORDEN: {order_id}")
    print(f"📅 Fecha: 21 jul 16:41 hs")
    print(f"🛍️ Producto: Zapatilla Bota Montagne Mujer Dynamo Trekking")
    print(f"👤 Cliente: Mariela Quiroz")
    print(f"📍 Destino: Puerto Deseado, Santa Cruz")
    print(f"📝 Nota: dEPO")
    
    order_data = get_order_full_details(order_id, access_token)
    
    if order_data:
        print("\n✅ Análisis completado")
        
        # Buscar el SKU problemático específicamente
        items = order_data.get('order_items', [])
        for item in items:
            seller_sku = item.get('item', {}).get('seller_sku')
            if seller_sku and 'NMIDKUDZDWOUT' in seller_sku:
                print(f"\n🎯 ENCONTRADO SKU PROBLEMÁTICO: {seller_sku}")
                item_id = item.get('item', {}).get('id')
                if item_id:
                    # Obtener seller_custom_field del item completo
                    item_details = analyze_item_details(item_id, None, access_token)
                    if item_details:
                        custom_field = item_details.get('seller_custom_field')
                        analyze_sku_pattern(seller_sku, custom_field)
    else:
        print("❌ No se pudo analizar la orden")

if __name__ == "__main__":
    main()
