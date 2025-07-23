#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Conversor de códigos de barras físicos a SKUs de MercadoLibre
"""

import re
from typing import Optional, List, Tuple
from utils.logger import get_logger

log = get_logger(__name__)

class BarcodeConverter:
    """Convierte códigos de barras físicos a SKUs de MercadoLibre."""
    
    # Mapeo de códigos de color físicos a códigos ML
    COLOR_MAP = {
        # Negro
        "NN0": "NN0",
        "NN": "NN0", 
        "NNO": "NN0",
        
        # Otros colores comunes
        "NC0": "NC0",
        "NC": "NC0",
        "MTP": "MTP",
        "RWF": "RWF",
        "VM0": "VM0",
        "AM0": "AM0",
        "AGC": "AGC",
        "AR0": "AR0",
    }
    
    # Patrones de códigos de barras conocidos
    BARCODE_PATTERNS = [
        # Patrón: BASE + COLOR + TALLE (ej: NMIDKUDZDWNN038)
        r'^([A-Z]{2}[A-Z0-9]{8})([A-Z]{2,3})(\d{2,3})$',
        
        # Patrón: BASE + COLOR + T + TALLE (ej: NMIDKUDZDWNN0T38)  
        r'^([A-Z]{2}[A-Z0-9]{8})([A-Z]{2,3})T(\d{2,3})$',
        
        # Patrón: BASE + COLOR + TALLE con MTP (ej: NMIDKUDZDWMTP40)
        r'^([A-Z]{2}[A-Z0-9]{8})([A-Z]{3})(\d{2,3})$',
    ]
    
    def __init__(self):
        """Inicializa el conversor."""
        pass
    
    def convert_barcode_to_sku(self, barcode: str) -> Optional[str]:
        """
        Convierte un código de barras físico a SKU de MercadoLibre.
        
        Args:
            barcode: Código de barras físico (ej: NMIDKUDZDWNN038)
            
        Returns:
            SKU de MercadoLibre (ej: NMIDKUDZDW-NN0-T38) o None si no se puede convertir
        """
        if not barcode:
            return None
            
        barcode = barcode.upper().strip()
        log.debug(f"🔄 Intentando convertir código: {barcode}")
        
        # Probar cada patrón
        for i, pattern in enumerate(self.BARCODE_PATTERNS):
            match = re.match(pattern, barcode)
            if match:
                base, color, talle = match.groups()
                
                log.debug(f"📋 Patrón {i+1} coincide:")
                log.debug(f"  Base: {base}")
                log.debug(f"  Color: {color}")
                log.debug(f"  Talle: {talle}")
                
                # Convertir color si es necesario
                converted_color = self.COLOR_MAP.get(color, color)
                
                # Formatear SKU ML
                ml_sku = f"{base}-{converted_color}-T{talle}"
                
                log.info(f"✅ Código convertido: {barcode} → {ml_sku}")
                return ml_sku
        
        log.debug(f"❌ No se pudo convertir código: {barcode}")
        return None
    
    def get_conversion_candidates(self, barcode: str) -> List[str]:
        """
        Genera múltiples candidatos de conversión para un código.
        
        Args:
            barcode: Código de barras físico
            
        Returns:
            Lista de posibles SKUs ML
        """
        candidates = []
        
        # Conversión principal
        main_conversion = self.convert_barcode_to_sku(barcode)
        if main_conversion:
            candidates.append(main_conversion)
        
        # Variaciones adicionales
        if len(barcode) >= 12:
            # Probar diferentes divisiones
            base_candidates = [
                barcode[:10],  # 10 caracteres de base
                barcode[:11],  # 11 caracteres de base
                barcode[:9],   # 9 caracteres de base
            ]
            
            for base in base_candidates:
                remaining = barcode[len(base):]
                if len(remaining) >= 4:
                    # Probar diferentes divisiones de color/talle
                    for color_len in [2, 3]:
                        if len(remaining) >= color_len + 2:
                            color = remaining[:color_len]
                            talle = remaining[color_len:]
                            
                            converted_color = self.COLOR_MAP.get(color, color)
                            candidate = f"{base}-{converted_color}-T{talle}"
                            
                            if candidate not in candidates:
                                candidates.append(candidate)
        
        return candidates
    
    def add_color_mapping(self, physical_color: str, ml_color: str):
        """
        Agrega un nuevo mapeo de color.
        
        Args:
            physical_color: Código de color en el código físico
            ml_color: Código de color en MercadoLibre
        """
        self.COLOR_MAP[physical_color.upper()] = ml_color.upper()
        log.info(f"📝 Mapeo de color agregado: {physical_color} → {ml_color}")

# Instancia global del conversor
barcode_converter = BarcodeConverter()

def convert_barcode_to_ml_sku(barcode: str) -> Optional[str]:
    """
    Función utilitaria para convertir código de barras a SKU ML.
    
    Args:
        barcode: Código de barras físico
        
    Returns:
        SKU de MercadoLibre o None
    """
    return barcode_converter.convert_barcode_to_sku(barcode)

def get_sku_candidates(barcode: str) -> List[str]:
    """
    Función utilitaria para obtener candidatos de SKU.
    
    Args:
        barcode: Código de barras físico
        
    Returns:
        Lista de posibles SKUs ML
    """
    return barcode_converter.get_conversion_candidates(barcode)
