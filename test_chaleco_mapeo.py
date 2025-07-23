#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Prueba el mapeo manual del chaleco Weis
"""

from services.picker_service import PickerService

def test_chaleco_mapeo():
    """Prueba el mapeo manual del código del chaleco."""
    print("🎒 PRUEBA MAPEO CHALECO WEIS")
    print("=" * 50)
    
    # Crear picker service
    picker = PickerService()
    
    # Códigos a probar
    test_codes = [
        "101-350-ML",      # Código físico del chaleco (NUEVO MAPEO)
        "7798333733209",   # Código del anafe (mapeo existente)
        "101-W350-WML",    # SKU real de MercadoLibre
        "codigo-inexistente",  # Código que no existe
    ]
    
    print("🔍 PROBANDO MAPEOS MANUALES:")
    print()
    
    for code in test_codes:
        print(f"📱 Escaneando código: '{code}'")
        
        try:
            # Usar la función del picker para buscar
            ml_code, barcode = picker.get_ml_code_from_barcode(code)
            
            if ml_code:
                print(f"  ✅ MAPEO ENCONTRADO:")
                print(f"    📋 SKU mapeado: {ml_code}")
                print(f"    📱 Código original: {barcode}")
                
                # Verificar si es el mapeo del chaleco
                if code == "101-350-ML" and ml_code == "101-W350-WML":
                    print(f"    🎒 ¡CHALECO WEIS MAPEADO CORRECTAMENTE!")
                    
            else:
                print(f"  ❌ No encontrado en mapeo manual ni SQL")
                
        except Exception as e:
            print(f"  ⚠️ Error: {e}")
        
        print()
    
    print("✅ Prueba de mapeo completada")

if __name__ == "__main__":
    test_chaleco_mapeo()
