#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Sistema de estadísticas diarias con cache
"""

import json
import os
from datetime import datetime, date
from typing import Dict, Any
from utils.logger import get_logger

log = get_logger(__name__)

class DailyStatsManager:
    """Maneja estadísticas diarias con cache persistente."""
    
    def __init__(self, cache_file: str = "daily_stats_cache.json"):
        """
        Inicializa el manager de estadísticas.
        
        Args:
            cache_file: Archivo donde guardar el cache
        """
        self.cache_file = cache_file
        self.stats_cache = self._load_cache()
    
    def _load_cache(self) -> Dict[str, Any]:
        """Carga el cache desde archivo."""
        try:
            if os.path.exists(self.cache_file):
                with open(self.cache_file, 'r', encoding='utf-8') as f:
                    cache = json.load(f)
                    log.debug(f"📊 Cache cargado desde {self.cache_file}")
                    return cache
        except Exception as e:
            log.warning(f"⚠️ Error cargando cache: {e}")
        
        # Cache vacío por defecto
        return {}
    
    def _save_cache(self):
        """Guarda el cache a archivo."""
        try:
            with open(self.cache_file, 'w', encoding='utf-8') as f:
                json.dump(self.stats_cache, f, indent=2, ensure_ascii=False)
                log.debug(f"💾 Cache guardado en {self.cache_file}")
        except Exception as e:
            log.error(f"❌ Error guardando cache: {e}")
    
    def _get_date_key(self, target_date: date = None) -> str:
        """Obtiene la clave de fecha para el cache."""
        if target_date is None:
            target_date = date.today()
        return target_date.strftime("%Y-%m-%d")
    
    def increment_packages_today(self, count: int = 1):
        """
        Incrementa el contador de paquetes de hoy.
        
        Args:
            count: Cantidad a incrementar (default: 1)
        """
        date_key = self._get_date_key()
        
        if date_key not in self.stats_cache:
            self.stats_cache[date_key] = {
                "packages_printed": 0,
                "packages_picked": 0,
                "last_updated": datetime.now().isoformat()
            }
        
        self.stats_cache[date_key]["packages_printed"] += count
        self.stats_cache[date_key]["last_updated"] = datetime.now().isoformat()
        
        self._save_cache()
        log.info(f"📦 Paquetes hoy incrementado: +{count} (Total: {self.stats_cache[date_key]['packages_printed']})")
    
    def increment_picked_today(self, count: int = 1):
        """
        Incrementa el contador de artículos pickeados de hoy.
        
        Args:
            count: Cantidad a incrementar (default: 1)
        """
        date_key = self._get_date_key()
        
        if date_key not in self.stats_cache:
            self.stats_cache[date_key] = {
                "packages_printed": 0,
                "packages_picked": 0,
                "last_updated": datetime.now().isoformat()
            }
        
        self.stats_cache[date_key]["packages_picked"] += count
        self.stats_cache[date_key]["last_updated"] = datetime.now().isoformat()
        
        self._save_cache()
        log.info(f"🎯 Artículos pickeados hoy: +{count} (Total: {self.stats_cache[date_key]['packages_picked']})")
    
    def get_packages_today(self) -> int:
        """
        Obtiene el número de paquetes impresos hoy.
        
        Returns:
            Número de paquetes impresos hoy
        """
        date_key = self._get_date_key()
        return self.stats_cache.get(date_key, {}).get("packages_printed", 0)
    
    def get_picked_today(self) -> int:
        """
        Obtiene el número de artículos pickeados hoy.
        
        Returns:
            Número de artículos pickeados hoy
        """
        date_key = self._get_date_key()
        return self.stats_cache.get(date_key, {}).get("packages_picked", 0)
    
    def get_stats_for_date(self, target_date: date) -> Dict[str, Any]:
        """
        Obtiene estadísticas para una fecha específica.
        
        Args:
            target_date: Fecha objetivo
            
        Returns:
            Diccionario con estadísticas
        """
        date_key = self._get_date_key(target_date)
        return self.stats_cache.get(date_key, {
            "packages_printed": 0,
            "packages_picked": 0,
            "last_updated": None
        })
    
    def get_recent_stats(self, days: int = 7) -> Dict[str, Dict[str, Any]]:
        """
        Obtiene estadísticas de los últimos N días.
        
        Args:
            days: Número de días hacia atrás
            
        Returns:
            Diccionario con estadísticas por fecha
        """
        recent_stats = {}
        today = date.today()
        
        for i in range(days):
            target_date = today - timedelta(days=i)
            date_key = self._get_date_key(target_date)
            recent_stats[date_key] = self.get_stats_for_date(target_date)
        
        return recent_stats
    
    def reset_today(self):
        """Resetea las estadísticas de hoy (para testing)."""
        date_key = self._get_date_key()
        if date_key in self.stats_cache:
            del self.stats_cache[date_key]
            self._save_cache()
            log.info(f"🔄 Estadísticas de hoy reseteadas")
    
    def cleanup_old_stats(self, keep_days: int = 30):
        """
        Limpia estadísticas antiguas del cache.
        
        Args:
            keep_days: Días a mantener (default: 30)
        """
        cutoff_date = date.today() - timedelta(days=keep_days)
        cutoff_key = self._get_date_key(cutoff_date)
        
        keys_to_remove = [key for key in self.stats_cache.keys() if key < cutoff_key]
        
        for key in keys_to_remove:
            del self.stats_cache[key]
        
        if keys_to_remove:
            self._save_cache()
            log.info(f"🧹 Limpiadas {len(keys_to_remove)} estadísticas antiguas")

# Instancia global del manager
daily_stats = DailyStatsManager()

# Funciones utilitarias
def increment_packages_today(count: int = 1):
    """Incrementa el contador de paquetes de hoy."""
    daily_stats.increment_packages_today(count)

def increment_picked_today(count: int = 1):
    """Incrementa el contador de artículos pickeados de hoy."""
    daily_stats.increment_picked_today(count)

def get_packages_today() -> int:
    """Obtiene el número de paquetes impresos hoy."""
    return daily_stats.get_packages_today()

def get_picked_today() -> int:
    """Obtiene el número de artículos pickeados hoy."""
    return daily_stats.get_picked_today()

def get_today_summary() -> str:
    """Obtiene un resumen de las estadísticas de hoy."""
    packages = get_packages_today()
    picked = get_picked_today()
    return f"📦 Paquetes: {packages} | 🎯 Pickeados: {picked}"
