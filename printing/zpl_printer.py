"""Envío de etiquetas ZPL a una impresora Zebra usando win32print"""

import win32print
import win32api
from utils import config
from utils.logger import get_logger

log = get_logger(__name__)

def print_zpl(zpl_data: bytes | str, printer_name: str | None = None) -> None:
    if printer_name is None:
        printer_name = config.PRINTER_NAME
    
    log.info("Intentando imprimir ZPL en impresora: %s", printer_name)
    log.debug("Datos ZPL a imprimir (primeros 200 chars): %s", str(zpl_data)[:200] if zpl_data else "None")
    
    try:
        h_printer = win32print.OpenPrinter(printer_name)
        log.debug("Impresora abierta exitosamente: %s", printer_name)
    except Exception as e:
        log.error("Error al abrir impresora '%s': %s", printer_name, str(e))
        raise
    
    try:
        job_info = ("ZPL Job", None, "RAW")
        h_job = win32print.StartDocPrinter(h_printer, 1, job_info)
        log.debug("Trabajo de impresión iniciado, job_id: %s", h_job)
        
        win32print.StartPagePrinter(h_printer)
        log.debug("Página de impresión iniciada")
        
        if isinstance(zpl_data, str):
            zpl_data = zpl_data.encode()
            log.debug("Datos ZPL convertidos de str a bytes")
        
        # Agregar comando de inicialización ZPL para asegurar que la impresora procese correctamente
        init_command = b"~JA"  # Comando para cancelar trabajos pendientes
        final_zpl = init_command + zpl_data
        
        bytes_written = win32print.WritePrinter(h_printer, final_zpl)
        log.info("Datos ZPL enviados a impresora: %d bytes escritos", bytes_written)
        
        win32print.EndPagePrinter(h_printer)
        log.debug("Página de impresión finalizada")
        
        win32print.EndDocPrinter(h_printer)
        log.debug("Trabajo de impresión finalizado")
        
        log.info("Impresión ZPL completada exitosamente")
        
    except Exception as e:
        log.error("Error durante la impresión ZPL: %s", str(e))
        raise
    finally:
        win32print.ClosePrinter(h_printer)
        log.debug("Impresora cerrada")
