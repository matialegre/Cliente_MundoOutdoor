#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Consulta directa a la base de datos para encontrar el chaleco Weis
"""

import sqlite3
import os

def query_chaleco_database():
    """Consulta la base de datos SQLite para encontrar el chaleco."""
    print("🎒 CONSULTA DIRECTA BASE DE DATOS - CHALECO WEIS")
    print("=" * 60)
    
    # Buscar archivos de base de datos
    db_files = []
    for root, dirs, files in os.walk("."):
        for file in files:
            if file.endswith(('.db', '.sqlite', '.sqlite3')):
                db_files.append(os.path.join(root, file))
    
    print(f"📁 Archivos de BD encontrados: {len(db_files)}")
    for db_file in db_files:
        print(f"  - {db_file}")
    
    if not db_files:
        print("❌ No se encontraron archivos de base de datos")
        return
    
    # Probar cada base de datos
    for db_file in db_files:
        print(f"\n🔍 CONSULTANDO: {db_file}")
        print("-" * 40)
        
        try:
            conn = sqlite3.connect(db_file)
            cursor = conn.cursor()
            
            # Obtener lista de tablas
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = cursor.fetchall()
            
            print(f"📋 Tablas encontradas: {len(tables)}")
            for table in tables:
                print(f"  - {table[0]}")
            
            # Buscar en cada tabla
            for table_name in [t[0] for t in tables]:
                print(f"\n🔍 Tabla: {table_name}")
                
                try:
                    # Obtener estructura de la tabla
                    cursor.execute(f"PRAGMA table_info({table_name});")
                    columns = cursor.fetchall()
                    
                    column_names = [col[1] for col in columns]
                    print(f"  Columnas: {', '.join(column_names)}")
                    
                    # Buscar columnas relevantes
                    sku_cols = [col for col in column_names if 'sku' in col.lower()]
                    barcode_cols = [col for col in column_names if any(word in col.lower() for word in ['barcode', 'codigo', 'bar', 'code'])]
                    
                    if sku_cols or barcode_cols:
                        print(f"  📊 Columnas relevantes:")
                        if sku_cols:
                            print(f"    SKU: {', '.join(sku_cols)}")
                        if barcode_cols:
                            print(f"    Barcode: {', '.join(barcode_cols)}")
                        
                        # Buscar registros con '101' o '350' o 'weis' o 'chaleco'
                        search_terms = ['101', '350', 'weis', 'chaleco', 'W350']
                        
                        for term in search_terms:
                            print(f"\n  🔍 Buscando '{term}':")
                            
                            # Construir consulta dinámica
                            where_conditions = []
                            for col in column_names:
                                where_conditions.append(f"{col} LIKE '%{term}%'")
                            
                            if where_conditions:
                                query = f"SELECT * FROM {table_name} WHERE {' OR '.join(where_conditions)} LIMIT 5;"
                                
                                try:
                                    cursor.execute(query)
                                    results = cursor.fetchall()
                                    
                                    if results:
                                        print(f"    ✅ {len(results)} resultados encontrados:")
                                        for i, row in enumerate(results):
                                            print(f"      {i+1}. {dict(zip(column_names, row))}")
                                    else:
                                        print(f"    ❌ No se encontraron resultados")
                                        
                                except Exception as e:
                                    print(f"    ⚠️ Error en consulta: {e}")
                    
                except Exception as e:
                    print(f"  ⚠️ Error consultando tabla {table_name}: {e}")
            
            conn.close()
            
        except Exception as e:
            print(f"❌ Error conectando a {db_file}: {e}")
    
    print(f"\n✅ Consulta completada")

if __name__ == "__main__":
    query_chaleco_database()
