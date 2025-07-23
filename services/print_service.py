"""Funciones relacionadas con la impresión.

Se desacoplan de la GUI para poder usarse tanto desde app_gui.py como desde
scripts de línea de comandos o tests.
"""
from __future__ import annotations

import os
import tempfile
import logging
import subprocess    #  ←  faltaba
log = logging.getLogger(__name__)
from typing import List, Dict

import win32api  # type: ignore  # pywin32
import win32print  # type: ignore
from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas

__all__ = [
    "print_zpl_raw",
    "print_list_pdf",
    "generate_and_open_pdf",
]


def _open_printer(printer_name: str):
    """Devuelve un handle a la impresora o lanza IOError."""
    try:
        return win32print.OpenPrinter(printer_name)
    except Exception as exc:
        raise IOError(f"No se pudo abrir la impresora '{printer_name}': {exc}") from exc


def print_zpl_raw(zpl: bytes, printer: str) -> None:
    """Envía ZPL crudo a la impresora indicada."""
    hprinter = _open_printer(printer)
    try:
        job = win32print.StartDocPrinter(hprinter, 1, ("Etiqueta ZPL", None, "RAW"))
        win32print.StartPagePrinter(hprinter)
        win32print.WritePrinter(hprinter, zpl)
        win32print.EndPagePrinter(hprinter)
        win32print.EndDocPrinter(hprinter)
    finally:
        win32print.ClosePrinter(hprinter)


def print_list_pdf(rows: List[Dict[str, str]], printer: str) -> None:
    """
    Genera un PDF con casillas para tildar y lo envía a la impresora de hojas.
    ➊  Intenta printto (impresión silenciosa) …
    ➋  …si falla, abre Notepad /pt con un TXT alineado.
    """
    if not printer:
        raise ValueError("No hay impresora de listas seleccionada")

    # ---------- generar PDF ----------
    fd, pdf_path = tempfile.mkstemp(suffix=".pdf")
    os.close(fd)

    width, height = A4
    c = canvas.Canvas(pdf_path, pagesize=A4)

    # Cabecera
    y = height - 40
    c.setFont("Helvetica-Bold", 18)
    c.drawString(40, y, "Lista de artículos pendientes")
    y -= 30

    # Encabezados de columnas (ahora con depósito)
    c.setFont("Helvetica-Bold", 12)
    c.drawString(40,  y, "Artículo")
    c.drawString(280, y, "Talle")
    c.drawString(330, y, "Color")
    c.drawString(400, y, "Cant")
    c.drawString(440, y, "Depósito")
    c.drawString(515, y, "✔")
    y -= 8
    c.line(40, y, width - 40, y)
    y -= 22

    # Detalle (fuente + pequeña para que entre)
    c.setFont("Helvetica", 11)
    MAX_ART_LEN = 35                     # Reducido para hacer espacio al depósito
    
    def wrap_text(text, max_len):
        """Divide el texto en líneas que no excedan max_len caracteres."""
        if not text or len(text) <= max_len:
            return [text or ""]
        
        lines = []
        words = text.split()
        current_line = ""
        
        for word in words:
            if len(current_line + " " + word) <= max_len:
                current_line += (" " if current_line else "") + word
            else:
                if current_line:
                    lines.append(current_line)
                current_line = word
        
        if current_line:
            lines.append(current_line)
        
        return lines
    
    for r in rows:
        if y < 80:                       # salto de página (más espacio para nombres largos)
            c.showPage()
            y = height - 60
            c.setFont("Helvetica", 11)

        art = (r.get("art") or "")
        talle = (r.get("talle") or "")[:6]
        color = (r.get("color") or "")[:10]
        cant = str(r.get("cant") or "")
        deposito = (r.get("deposito") or "N/A")[:10]  # Nueva información de depósito
        
        # Dividir el nombre del artículo en líneas si es necesario
        art_lines = wrap_text(art, MAX_ART_LEN)
        
        # Dibujar la primera línea del artículo junto con talle, color, cantidad y depósito
        c.drawString(40, y, art_lines[0] if art_lines else "")
        c.drawString(280, y, talle)
        c.drawString(330, y, color)
        c.drawRightString(435, y, cant)
        c.drawString(440, y, deposito)  # Nueva columna depósito
        c.rect(515, y - 4, 14, 14)       # casilla para tick
        
        # Dibujar líneas adicionales del artículo si las hay
        for additional_line in art_lines[1:]:
            y -= 15  # Espaciado entre líneas del mismo artículo
            if y < 50:  # Verificar salto de página
                c.showPage()
                y = height - 60
                c.setFont("Helvetica", 11)
            c.drawString(40, y, additional_line)
        
        y -= 20  # Espaciado entre artículos diferentes

    c.save()
    log.info("PDF listo: %s", pdf_path)

    # ---------- Abrir PDF automáticamente para impresión manual ----------
    try:
        # Abrir el PDF con el programa predeterminado (Chrome, Edge, Acrobat, etc.)
        win32api.ShellExecute(
            0,            # hwnd
            "open",       # verbo para abrir (no imprimir)
            pdf_path,
            None,         # sin parámetros adicionales
            ".",
            1             # mostrar ventana
        )
        log.info("PDF abierto automáticamente para impresión manual: %s", pdf_path)
        return
    except Exception as exc:
        log.error("No se pudo abrir el PDF automáticamente: %s", exc)
        raise IOError(
            "No se pudo abrir el PDF automáticamente. Abre e imprime manualmente:\n"
            f"{pdf_path}"
        ) from exc


def generate_and_open_pdf(rows: List[Dict[str, str]]) -> None:
    """
    Genera un PDF con casillas para tildar, incluyendo columna de depósito,
    y lo abre automáticamente en el explorador para impresión manual.
    """
    if not rows:
        raise ValueError("No hay filas para generar el PDF")

    # ---------- generar PDF ----------
    fd, pdf_path = tempfile.mkstemp(suffix=".pdf")
    os.close(fd)

    width, height = A4
    c = canvas.Canvas(pdf_path, pagesize=A4)

    # Cabecera
    y = height - 40
    c.setFont("Helvetica-Bold", 18)
    c.drawString(40, y, "Lista de artículos pendientes por depósito")
    y -= 30

    # Encabezados de columnas (ahora con depósito)
    c.setFont("Helvetica-Bold", 12)
    c.drawString(40,  y, "Artículo")
    c.drawString(280, y, "Talle")
    c.drawString(330, y, "Color")
    c.drawString(400, y, "Cant")
    c.drawString(440, y, "Depósito")
    c.drawString(515, y, "✔")
    y -= 8
    c.line(40, y, width - 40, y)
    y -= 22

    # Detalle (fuente + pequeña para que entre)
    c.setFont("Helvetica", 11)
    MAX_ART_LEN = 35  # Reducido para hacer espacio al depósito
    
    for r in rows:
        if y < 50:  # salto de página
            c.showPage()
            y = height - 60
            c.setFont("Helvetica", 11)

        art = (r.get("art") or "")
        if len(art) > MAX_ART_LEN:
            art = art[:MAX_ART_LEN - 2] + "…"

        c.drawString(40,  y, art)
        c.drawString(280, y, (r.get("talle")  or "")[:6])
        c.drawString(330, y, (r.get("color")  or "")[:10])
        c.drawRightString(435, y, str(r.get("cant") or ""))
        c.drawString(440, y, (r.get("deposito") or "N/A")[:10])  # Nueva columna depósito
        c.rect(515, y - 4, 14, 14)  # casilla para tick
        y -= 20

    c.save()
    log.info("PDF generado: %s", pdf_path)

    # ---------- abrir PDF en el explorador ----------
    try:
        # Usar el comando del sistema para abrir el PDF
        os.startfile(pdf_path)  # Windows
        log.info("PDF abierto en el explorador: %s", pdf_path)
    except Exception as exc:
        log.error("No se pudo abrir el PDF automáticamente: %s", exc)
        raise IOError(f"PDF generado pero no se pudo abrir automáticamente:\n{pdf_path}") from exc