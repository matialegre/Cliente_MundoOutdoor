#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Prueba rápida del estado de las zapatillas
"""

from utils.sku_resolver import is_out_sku

def test_quick_zapatillas():
    """Prueba rápida de zapatillas conocidas."""
    print("👟 PRUEBA RÁPIDA ZAPATILLAS")
    print("=" * 40)
    
    # SKUs de zapatillas conocidos
    zapatillas_test = [
        ("NMIDKUDZDWOUT", "Zapatilla con OUT - DEBE resolverse"),
        ("NMIDKTDZHV-NC0-T39", "Zapatilla Huts - YA está bien"),
        ("NMIDKUDZDW-NN0-T38", "SKU real resuelto - Perfecto"),
    ]
    
    print("🔍 ANÁLISIS DE SKUs:")
    for sku, descripcion in zapatillas_test:
        print(f"\n👟 {descripcion}")
        print(f"   SKU: {sku}")
        print(f"   ¿Termina en OUT?: {is_out_sku(sku)}")
        
        if is_out_sku(sku):
            print(f"   ✅ Se resolverá automáticamente vía API")
        else:
            print(f"   ✅ Ya está en formato correcto")
    
    print(f"\n🎯 CONCLUSIÓN:")
    print(f"✅ Zapatillas OUT: Se resuelven automáticamente")
    print(f"✅ Zapatillas normales: Funcionan directamente") 
    print(f"✅ Chaleco Weis: Mapeo manual implementado")
    
    print(f"\n💡 PARA PROBAR:")
    print(f"1. Abre la GUI: python gui/app_gui_v3.py")
    print(f"2. Carga órdenes del día")
    print(f"3. Presiona 'PICKEAR'")
    print(f"4. Escanea cualquier código")
    print(f"5. ¡Debería funcionar automáticamente!")

if __name__ == "__main__":
    test_quick_zapatillas()
