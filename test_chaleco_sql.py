#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Prueba si el SKU del chaleco Weis está en la base de datos SQL
"""

from services.picker_service import PickerService

def test_chaleco_sql():
    """Prueba búsqueda del chaleco en SQL."""
    print("🎒 PRUEBA CHALECO WEIS EN SQL")
    print("=" * 50)
    
    # Crear picker service
    picker = PickerService()
    
    # SKUs a probar
    test_skus = [
        "101-W350-WML",    # SKU real de MercadoLibre
        "101-350-ML",      # Código de barras físico
        "101W350WML",      # Sin guiones
        "101-W350",        # Sin talle
        "W350-WML",        # Sin prefijo
    ]
    
    print("🔍 PROBANDO SKUs EN BASE DE DATOS:")
    print()
    
    for sku in test_skus:
        print(f"🏷️ Probando SKU: '{sku}'")
        
        try:
            # Usar la función del picker para buscar en SQL
            ml_code, barcode = picker.get_ml_code_from_barcode(sku)
            
            if ml_code or barcode:
                print(f"  ✅ ENCONTRADO:")
                print(f"    ML Code: {ml_code}")
                print(f"    Barcode: {barcode}")
            else:
                print(f"  ❌ No encontrado en SQL")
                
        except Exception as e:
            print(f"  ⚠️ Error: {e}")
        
        print()
    
    # También probar búsqueda inversa
    print("🔄 BÚSQUEDA INVERSA - Códigos de barras que contengan '101' o '350':")
    print()
    
    try:
        # Esto requeriría una consulta SQL personalizada
        # Por ahora solo mostramos el concepto
        print("  💡 Para búsqueda inversa necesitaríamos consultar:")
        print("     SELECT * FROM productos WHERE sku LIKE '%101%' OR sku LIKE '%350%'")
        print("     O WHERE barcode LIKE '%101%' OR barcode LIKE '%350%'")
        
    except Exception as e:
        print(f"  ⚠️ Error en búsqueda inversa: {e}")

if __name__ == "__main__":
    test_chaleco_sql()
