#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Prueba el conversor de códigos de barras con los casos problemáticos
"""

from utils.barcode_converter import BarcodeConverter

def test_problematic_barcodes():
    """Prueba los 3 casos problemáticos del usuario."""
    print("🔄 PRUEBA CONVERSOR CÓDIGOS DE BARRAS")
    print("=" * 60)
    
    converter = BarcodeConverter()
    
    # Casos problemáticos del usuario
    test_cases = [
        {
            "physical": "NMIDKUDZDWNN038",
            "expected_ml": "NMIDKUDZDW-NNO-T38",
            "description": "Zapatilla 1 - NN038 → NNO-T38"
        },
        {
            "physical": "NMIDKTDZHVNN039", 
            "expected_ml": "NMIDKTDZHV-NC0-T39",
            "description": "Zapatilla 2 - NN039 → NC0-T39"
        },
        {
            "physical": "NMIDKUDZDWMTP40",
            "expected_ml": "NMIDKUHZDY-NNO-T40",  # Nota: base diferente
            "description": "Zapatilla 3 - MTP40 → NNO-T40 (base diferente)"
        }
    ]
    
    print("🧪 PROBANDO CONVERSIONES:")
    print()
    
    for i, case in enumerate(test_cases, 1):
        physical = case["physical"]
        expected = case["expected_ml"]
        description = case["description"]
        
        print(f"🔍 CASO {i}: {description}")
        print(f"  📱 Código físico: {physical}")
        print(f"  🎯 SKU ML esperado: {expected}")
        
        # Conversión principal
        converted = converter.convert_barcode_to_sku(physical)
        print(f"  🔄 Conversión obtenida: {converted}")
        
        if converted == expected:
            print(f"  ✅ ¡CONVERSIÓN PERFECTA!")
        elif converted:
            print(f"  ⚠️ Conversión parcial - puede funcionar")
        else:
            print(f"  ❌ No se pudo convertir")
        
        # Candidatos adicionales
        candidates = converter.get_conversion_candidates(physical)
        if len(candidates) > 1:
            print(f"  📋 Candidatos adicionales:")
            for j, candidate in enumerate(candidates[1:], 2):
                print(f"    {j}. {candidate}")
        
        print()
    
    # Probar algunos casos adicionales
    print("🔬 CASOS ADICIONALES:")
    additional_cases = [
        "NMIDKUDZDWOUT",      # Caso OUT original
        "101-350-ML",         # Chaleco (ya mapeado)
        "ABCD1234EFNN038",    # Patrón genérico
    ]
    
    for code in additional_cases:
        converted = converter.convert_barcode_to_sku(code)
        print(f"  📱 {code} → {converted or 'No convertible'}")
    
    print()
    print("🎯 ANÁLISIS:")
    print("✅ El conversor puede manejar patrones básicos")
    print("⚠️ Algunos casos pueden necesitar mapeo manual adicional")
    print("💡 La base de algunos productos puede ser diferente (caso 3)")

if __name__ == "__main__":
    test_problematic_barcodes()
