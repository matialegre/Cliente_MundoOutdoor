"""Cache diario para evitar operaciones duplicadas y optimizar rendimiento."""

from datetime import datetime
from typing import Set, Dict, Any
import json
import os
from pathlib import Path

class DailyCache:
    """Cache que se resetea diariamente para controlar operaciones únicas por día."""
    
    def __init__(self):
        self.cache_dir = Path(__file__).parent.parent / "cache"
        self.cache_dir.mkdir(exist_ok=True)
        
        # Cache de descuentos de stock mejorado: (pack_id, sku, unit_index) -> timestamp
        self._stock_discounts: Dict[tuple, str] = {}
        
        # Cache de códigos de barra buscados en SQL
        self._barcode_cache: Dict[str, Any] = {}
        
        # Artículos pickeados hoy
        self._picked_items: list = []
        
        self._current_date = datetime.now().date()
        self._load_cache()
    
    def _get_cache_file(self) -> Path:
        """Obtiene el archivo de cache para la fecha actual."""
        date_str = self._current_date.strftime("%Y-%m-%d")
        return self.cache_dir / f"daily_cache_{date_str}.json"
    
    def _load_cache(self):
        """Carga el cache del día actual desde disco."""
        cache_file = self._get_cache_file()
        if cache_file.exists():
            try:
                with open(cache_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    
                # Convertir a dict para stock_discounts mejorado
                stock_data = data.get('stock_discounts', {})
                if isinstance(stock_data, list):
                    # Compatibilidad con formato anterior
                    self._stock_discounts = {tuple(item): datetime.now().strftime("%H:%M:%S") for item in stock_data}
                else:
                    # Nuevo formato dict
                    self._stock_discounts = {tuple(k.split('|')): v for k, v in stock_data.items()}
                self._barcode_cache = data.get('barcode_cache', {})
                self._picked_items = data.get('picked_items', [])
                
            except Exception as e:
                print(f"Error cargando cache diario: {e}")
                self._reset_cache()
    
    def _save_cache(self):
        """Guarda el cache actual a disco."""
        try:
            cache_file = self._get_cache_file()
            # Convertir dict de descuentos a formato serializable
            stock_data = {'|'.join(k): v for k, v in self._stock_discounts.items()}
            data = {
                'stock_discounts': stock_data,
                'barcode_cache': self._barcode_cache,
                'picked_items': self._picked_items
            }
            
            with open(cache_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
                
        except Exception as e:
            print(f"Error guardando cache diario: {e}")
    
    def _save_cache_immediate(self):
        """Guarda el cache inmediatamente con manejo robusto de errores."""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                cache_file = self._get_cache_file()
                # Crear directorio si no existe
                cache_file.parent.mkdir(parents=True, exist_ok=True)
                
                # Convertir dict de descuentos a formato serializable
                stock_data = {'|'.join(k): v for k, v in self._stock_discounts.items()}
                data = {
                    'stock_discounts': stock_data,
                    'barcode_cache': self._barcode_cache,
                    'picked_items': self._picked_items,
                    'last_saved': datetime.now().isoformat()
                }
                
                # Escribir a archivo temporal primero
                temp_file = cache_file.with_suffix('.tmp')
                with open(temp_file, 'w', encoding='utf-8') as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)
                
                # Mover archivo temporal al final (operación atómica)
                temp_file.replace(cache_file)
                return  # Éxito
                
            except Exception as e:
                if attempt == max_retries - 1:
                    print(f"ERROR CRÍTICO: No se pudo guardar cache tras {max_retries} intentos: {e}")
                else:
                    print(f"Intento {attempt + 1} falló, reintentando: {e}")
                    import time
                    time.sleep(0.1)  # Breve pausa antes de reintentar
    
    def _reset_cache(self):
        """Resetea el cache (nuevo día)."""
        self._stock_discounts.clear()
        self._barcode_cache.clear()
        self._picked_items.clear()
    
    def _check_date_change(self):
        """Verifica si cambió el día y resetea cache si es necesario."""
        current_date = datetime.now().date()
        if current_date != self._current_date:
            self._current_date = current_date
            self._reset_cache()
    
    # --- Métodos para descuentos de stock ---
    
    def is_stock_already_discounted(self, pack_id: str, sku: str, unit_index: int = 0) -> bool:
        """Verifica si ya se descontó stock para este pack_id + sku + unit_index hoy."""
        self._check_date_change()
        return (pack_id, sku, str(unit_index)) in self._stock_discounts
    
    def mark_stock_discounted(self, pack_id: str, sku: str, unit_index: int = 0):
        """Marca que se descontó stock para este pack_id + sku + unit_index."""
        self._check_date_change()
        timestamp = datetime.now().strftime("%H:%M:%S")
        self._stock_discounts[(pack_id, sku, str(unit_index))] = timestamp
        # Persistencia inmediata crítica para descuentos de stock
        self._save_cache_immediate()
    
    def get_stock_discount_count(self, pack_id: str, sku: str) -> int:
        """Obtiene cuántas unidades ya se descontaron para este pack_id + sku."""
        self._check_date_change()
        count = 0
        for key in self._stock_discounts.keys():
            if key[0] == pack_id and key[1] == sku:
                count += 1
        return count
    
    # --- Métodos para cache de códigos de barra ---
    
    def get_barcode_info(self, barcode: str) -> Any:
        """Obtiene información de código de barra desde cache."""
        self._check_date_change()
        return self._barcode_cache.get(barcode)
    
    def set_barcode_info(self, barcode: str, info: Any):
        """Guarda información de código de barra en cache."""
        self._check_date_change()
        self._barcode_cache[barcode] = info
        # Persistencia inmediata para operaciones críticas
        self._save_cache_immediate()
    
    # --- Métodos para artículos pickeados ---
    
    def add_picked_item(self, pack_id: str, sku: str, title: str, timestamp: str = None):
        """Agrega un artículo pickeado al historial del día."""
        self._check_date_change()
        if timestamp is None:
            timestamp = datetime.now().strftime("%H:%M:%S")
            
        item = {
            'pack_id': pack_id,
            'sku': sku,
            'title': title,
            'timestamp': timestamp
        }
        self._picked_items.append(item)
        self._save_cache()
    
    def get_picked_items_today(self) -> list:
        """Obtiene lista de artículos pickeados hoy."""
        self._check_date_change()
        return self._picked_items.copy()
    
    def get_picked_count_today(self) -> int:
        """Obtiene cantidad de artículos pickeados hoy."""
        self._check_date_change()
        return len(self._picked_items)

# Instancia global
daily_cache = DailyCache()
