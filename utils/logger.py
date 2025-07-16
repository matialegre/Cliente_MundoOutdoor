"""Configuración de logging centralizado"""
import logging
from pathlib import Path

LOGS_DIR = Path(__file__).resolve().parent.parent / 'logs'
LOGS_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
    handlers=[
        logging.FileHandler(LOGS_DIR / 'app.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

get_logger = logging.getLogger
