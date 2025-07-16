"""Envío de etiquetas ZPL a una impresora Zebra usando win32print"""

import win32print
import win32api
from utils import config


def print_zpl(zpl_data: bytes | str, printer_name: str | None = None) -> None:
    if printer_name is None:
        printer_name = config.PRINTER_NAME
    h_printer = win32print.OpenPrinter(printer_name)
    try:
        job_info = ("ZPL Job", None, "RAW")
        h_job = win32print.StartDocPrinter(h_printer, 1, job_info)
        win32print.StartPagePrinter(h_printer)
        if isinstance(zpl_data, str):
            zpl_data = zpl_data.encode()
        win32print.WritePrinter(h_printer, zpl_data)
        win32print.EndPagePrinter(h_printer)
        win32print.EndDocPrinter(h_printer)
    finally:
        win32print.ClosePrinter(h_printer)
