#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Prueba completa del picking del chaleco Weis
"""

from services.picker_service import PickerService
from datetime import datetime, date

def test_chaleco_picking():
    """Prueba el flujo completo de picking del chaleco."""
    print("🎒 PRUEBA COMPLETA PICKING CHALECO WEIS")
    print("=" * 60)
    
    # Crear picker service
    picker = PickerService()
    
    print("🔧 Inicializando picker service...")
    
    try:
        # Cargar órdenes del día donde debería estar el chaleco
        # Basándonos en el análisis anterior, probemos días recientes
        today = date.today()
        print(f"📅 Cargando órdenes de: {today}")
        
        orders = picker.load_orders(today, today)
        print(f"📦 {len(orders)} órdenes cargadas")
        
        # Buscar órdenes que contengan "chaleco" o "weis" o el SKU
        chaleco_orders = []
        for order in orders:
            for item in order.items:
                item_title = item.title.lower() if item.title else ""
                item_sku = item.sku or ""
                item_real_sku = getattr(item, 'real_sku', item.sku) or ""
                
                if any(term in item_title for term in ['chaleco', 'weis', 'hidratacion']) or \
                   any(term in item_sku for term in ['101-W350', '101-350']) or \
                   any(term in item_real_sku for term in ['101-W350', '101-350']):
                    chaleco_orders.append((order, item))
        
        if chaleco_orders:
            print(f"\n🎒 {len(chaleco_orders)} órdenes con chaleco encontradas:")
            for i, (order, item) in enumerate(chaleco_orders):
                print(f"  {i+1}. Orden: {order.id}")
                print(f"     Producto: {item.title}")
                print(f"     SKU: {item.sku}")
                print(f"     SKU Real: {getattr(item, 'real_sku', 'N/A')}")
                print(f"     Estado: {order.shipping_substatus}")
                print(f"     Notas: {order.notes}")
                print()
        else:
            print(f"\n❌ No se encontraron órdenes con chaleco en {today}")
            print(f"💡 Puede que la orden sea de otro día")
        
        # Probar el escaneo del código físico
        print(f"🔍 SIMULANDO ESCANEO DEL CÓDIGO FÍSICO:")
        print(f"📱 Código escaneado: '101-350-ML'")
        
        # Simular el proceso de escaneo
        barcode_scanned = "101-350-ML"
        success, message = picker.scan_barcode(barcode_scanned)
        
        print(f"📊 Resultado del escaneo:")
        print(f"  ✅ Éxito: {success}")
        print(f"  💬 Mensaje: {message}")
        
        if success:
            print(f"  🎉 ¡CHALECO PICKEADO EXITOSAMENTE!")
        else:
            print(f"  ⚠️ Razón del fallo: {message}")
            
            # Si falló, mostrar información adicional
            if "NO ESTÁ PARA PICKEAR" in message:
                print(f"  💡 Esto puede significar:")
                print(f"     - La orden no está en estado 'ready_to_print'")
                print(f"     - La orden no tiene nota 'DEPOSITO'")
                print(f"     - La orden es de otro día")
        
    except Exception as e:
        print(f"❌ Error durante la prueba: {e}")
        import traceback
        traceback.print_exc()
    
    print(f"\n✅ Prueba de picking completada")

if __name__ == "__main__":
    test_chaleco_picking()
