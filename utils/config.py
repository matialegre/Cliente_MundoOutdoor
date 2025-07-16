"""Carga de configuración desde variables de entorno usando python-dotenv"""
from pathlib import Path
from dotenv import load_dotenv
import os

# Carga .env en el directorio raíz del proyecto
env_path = Path(__file__).resolve().parent.parent / '.env'
load_dotenv(dotenv_path=env_path, override=False)

ML_CLIENT_ID = os.getenv('ML_CLIENT_ID')
ML_CLIENT_SECRET = os.getenv('ML_CLIENT_SECRET')
ML_REFRESH_TOKEN = os.getenv('ML_REFRESH_TOKEN')
DRAGONFISH_TOKEN = os.getenv('DRAGONFISH_TOKEN')
SQL_CONN_STR = os.getenv('SQL_CONN_STR')
PRINTER_NAME = os.getenv('PRINTER_NAME', 'Xprinter XP-410B')

if not ML_CLIENT_ID:
    print("⚠️  ML_CLIENT_ID no definido. Usa .env o variables de entorno.")
