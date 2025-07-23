#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Verifica el estado actual de las zapatillas y SKU resolver
"""

from services.picker_service import PickerService
from utils.sku_resolver import is_out_sku
from datetime import date

def test_zapatillas_status():
    """Verifica el estado de las zapatillas con y sin OUT."""
    print("👟 VERIFICACIÓN ESTADO ZAPATILLAS")
    print("=" * 50)
    
    # Crear picker service
    picker = PickerService()
    
    print("🔧 Inicializando picker service...")
    
    try:
        # Cargar órdenes de hoy
        today = date.today()
        print(f"📅 Cargando órdenes de: {today}")
        
        orders = picker.load_orders(today, today)
        print(f"📦 {len(orders)} órdenes cargadas")
        
        # Buscar zapatillas
        zapatillas_found = []
        out_skus_found = []
        
        for order in orders:
            for item in order.items:
                item_title = item.title.lower() if item.title else ""
                item_sku = item.sku or ""
                item_real_sku = getattr(item, 'real_sku', item.sku) or ""
                
                # Buscar zapatillas
                if any(term in item_title for term in ['zapatilla', 'bota', 'zapato']):
                    zapatillas_found.append((order, item))
                
                # Buscar SKUs con OUT
                if is_out_sku(item_sku):
                    out_skus_found.append((order, item))
        
        # Mostrar zapatillas encontradas
        if zapatillas_found:
            print(f"\n👟 {len(zapatillas_found)} zapatillas encontradas:")
            for i, (order, item) in enumerate(zapatillas_found[:5]):  # Mostrar solo las primeras 5
                print(f"  {i+1}. Orden: {order.id}")
                print(f"     Producto: {item.title}")
                print(f"     SKU original: {item.sku}")
                print(f"     SKU real: {getattr(item, 'real_sku', 'N/A')}")
                print(f"     ¿Termina en OUT?: {is_out_sku(item.sku or '')}")
                print(f"     Estado: {order.shipping_substatus}")
                print(f"     Depósito: {'DEPOSITO' if order.notes and 'DEPOSITO' in order.notes else 'OTRO'}")
                print()
        
        # Mostrar SKUs con OUT
        if out_skus_found:
            print(f"\n🔍 {len(out_skus_found)} SKUs con sufijo OUT encontrados:")
            for i, (order, item) in enumerate(out_skus_found[:3]):  # Mostrar solo los primeros 3
                print(f"  {i+1}. SKU OUT: {item.sku}")
                print(f"     SKU resuelto: {getattr(item, 'real_sku', 'N/A')}")
                print(f"     Producto: {item.title}")
                print(f"     ¿Resuelto correctamente?: {getattr(item, 'real_sku', '') != item.sku}")
                print()
        
        # Probar algunos códigos de zapatillas conocidos
        print(f"🧪 PROBANDO CÓDIGOS DE ZAPATILLAS CONOCIDOS:")
        test_codes = [
            "NMIDKUDZDWOUT",      # Zapatilla con OUT (del análisis anterior)
            "NMIDKTDZHV-NC0-T39", # Zapatilla Huts (del análisis)
            "NMIDKUDZDW-NN0-T38", # SKU real resuelto
        ]
        
        for code in test_codes:
            print(f"\n📱 Probando código: '{code}'")
            
            # Verificar si es OUT
            if is_out_sku(code):
                print(f"  🔍 Es SKU con OUT - debería resolverse automáticamente")
            
            # Probar mapeo manual
            ml_code, barcode = picker.get_ml_code_from_barcode(code)
            if ml_code:
                print(f"  ✅ Encontrado en mapeo/SQL: {ml_code}")
            else:
                print(f"  ❌ No encontrado en mapeo manual ni SQL")
                print(f"  💡 Puede necesitar mapeo manual si el código físico es diferente")
        
        print(f"\n🎯 RESUMEN:")
        print(f"✅ SKU Resolver: {'Activo' if picker.sku_resolver else 'Inactivo'}")
        print(f"👟 Zapatillas encontradas: {len(zapatillas_found)}")
        print(f"🔍 SKUs OUT encontrados: {len(out_skus_found)}")
        print(f"📱 Mapeo manual chaleco: Implementado")
        
        if out_skus_found:
            print(f"💡 Las zapatillas OUT deberían mostrar el SKU real en la GUI")
        
    except Exception as e:
        print(f"❌ Error durante la verificación: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_zapatillas_status()
