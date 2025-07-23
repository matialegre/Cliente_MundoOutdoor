#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Script de prueba para validar la resolución de SKUs reales
para productos con sufijo OUT
"""

import logging
from datetime import datetime, timedelta
from services.picker_service import PickerService
from utils.sku_resolver import is_out_sku

# Configurar logging para ver todo el proceso
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def test_sku_resolution():
    """Prueba la resolución de SKUs reales en el sistema completo."""
    print("🧪 PRUEBA DE RESOLUCIÓN DE SKUs REALES")
    print("=" * 50)
    
    # Inicializar picker service
    picker = PickerService()
    
    # Cargar órdenes de los últimos 3 días para encontrar zapatillas OUT
    date_to = datetime.now().date()
    date_from = date_to - timedelta(days=3)
    
    print(f"📅 Cargando órdenes desde {date_from} hasta {date_to}")
    
    try:
        orders = picker.load_orders(date_from, date_to)
        print(f"📦 Cargadas {len(orders)} órdenes")
        
        # Buscar y analizar items con SKU OUT
        out_items = []
        resolved_items = []
        
        for order in orders:
            for item in order.items:
                if is_out_sku(item.sku):
                    out_items.append((order, item))
                    
                    # Verificar si se resolvió correctamente
                    real_sku = getattr(item, 'real_sku', None)
                    if real_sku and real_sku != item.sku:
                        resolved_items.append((order, item, real_sku))
        
        print(f"\n🔍 RESULTADOS:")
        print(f"  Items con sufijo OUT encontrados: {len(out_items)}")
        print(f"  Items con SKU real resuelto: {len(resolved_items)}")
        
        if out_items:
            print(f"\n📋 EJEMPLOS DE SKUs OUT:")
            for i, (order, item) in enumerate(out_items[:5]):  # Mostrar primeros 5
                real_sku = getattr(item, 'real_sku', 'No resuelto')
                print(f"  {i+1}. Orden: {order.id}")
                print(f"     Producto: {item.title[:50]}...")
                print(f"     SKU Original: {item.sku}")
                print(f"     SKU Real: {real_sku}")
                print(f"     Item ID: {item.item_id}")
                print(f"     Variation ID: {item.variation_id}")
                print()
        
        if resolved_items:
            print(f"✅ RESOLUCIONES EXITOSAS:")
            for i, (order, item, real_sku) in enumerate(resolved_items[:3]):  # Mostrar primeras 3
                print(f"  {i+1}. {item.sku} → {real_sku}")
                print(f"     Producto: {item.title[:40]}...")
                print()
        
        # Probar búsqueda por SKU real
        if resolved_items:
            print("🔍 PROBANDO BÚSQUEDA POR SKU REAL:")
            test_order, test_item, test_real_sku = resolved_items[0]
            
            # Simular escaneo con SKU real
            print(f"  Probando escaneo con SKU real: {test_real_sku}")
            
            # Inicializar sesión de picking con las órdenes
            picker.start_pick_session(orders)
            
            # Intentar escanear el SKU real
            success, message = picker.scan_barcode(test_real_sku)
            print(f"  Resultado: {'✅ ÉXITO' if success else '❌ FALLO'}")
            print(f"  Mensaje: {message}")
        
        print(f"\n🎯 RESUMEN:")
        print(f"  Total órdenes: {len(orders)}")
        print(f"  Items OUT: {len(out_items)}")
        print(f"  Resoluciones: {len(resolved_items)}")
        
        if out_items:
            resolution_rate = (len(resolved_items) / len(out_items)) * 100
            print(f"  Tasa de resolución: {resolution_rate:.1f}%")
        
        return len(resolved_items) > 0
        
    except Exception as e:
        print(f"❌ Error en la prueba: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_specific_zapatilla():
    """Prueba específica con la zapatilla que analizamos."""
    print("\n🎯 PRUEBA ESPECÍFICA - ZAPATILLA DYNAMO")
    print("=" * 50)
    
    picker = PickerService()
    
    # Cargar órdenes del 21 de julio
    date_target = datetime(2025, 7, 21).date()
    
    try:
        orders = picker.load_orders(date_target, date_target)
        print(f"📦 Cargadas {len(orders)} órdenes del {date_target}")
        
        # Buscar la zapatilla específica
        target_sku = "NMIDKUDZDWOUT"
        found_item = None
        found_order = None
        
        for order in orders:
            for item in order.items:
                if item.sku == target_sku:
                    found_item = item
                    found_order = order
                    break
            if found_item:
                break
        
        if found_item:
            print(f"✅ Zapatilla encontrada!")
            print(f"  Orden: {found_order.id}")
            print(f"  Producto: {found_item.title}")
            print(f"  SKU Original: {found_item.sku}")
            print(f"  SKU Real: {getattr(found_item, 'real_sku', 'No resuelto')}")
            print(f"  Item ID: {found_item.item_id}")
            print(f"  Variation ID: {found_item.variation_id}")
            
            # Probar escaneo
            real_sku = getattr(found_item, 'real_sku', None)
            if real_sku and real_sku != found_item.sku:
                print(f"\n🔍 Probando escaneo con SKU real: {real_sku}")
                picker.start_pick_session([found_order])
                success, message = picker.scan_barcode(real_sku)
                print(f"  Resultado: {'✅ ÉXITO' if success else '❌ FALLO'}")
                print(f"  Mensaje: {message}")
            else:
                print(f"⚠️ SKU real no disponible o igual al original")
        else:
            print(f"❌ Zapatilla {target_sku} no encontrada en {date_target}")
            
            # Mostrar algunos SKUs disponibles
            print(f"\n📋 SKUs disponibles en {date_target}:")
            count = 0
            for order in orders[:5]:  # Primeras 5 órdenes
                for item in order.items:
                    if item.sku:
                        print(f"  - {item.sku} ({item.title[:30]}...)")
                        count += 1
                        if count >= 10:
                            break
                if count >= 10:
                    break
        
    except Exception as e:
        print(f"❌ Error en prueba específica: {e}")
        import traceback
        traceback.print_exc()

def main():
    """Función principal de prueba."""
    print("🚀 INICIANDO PRUEBAS DE RESOLUCIÓN DE SKUs")
    print("=" * 60)
    
    # Prueba general
    general_success = test_sku_resolution()
    
    # Prueba específica
    test_specific_zapatilla()
    
    print(f"\n🏁 PRUEBAS COMPLETADAS")
    print(f"Resolución general: {'✅ FUNCIONANDO' if general_success else '❌ PROBLEMAS'}")
    
    return general_success

if __name__ == "__main__":
    main()
