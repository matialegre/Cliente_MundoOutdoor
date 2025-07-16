"""Punto de entrada de la aplicación.

Decide si ejecutar la GUI (ttkbootstrap) o una interfaz de línea de comandos.
Por defecto lanza la GUI.
"""

import argparse
import sys
from gui.app_gui import launch_gui


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Cliente Matías")
    parser.add_argument("--cli", action="store_true", help="Ejecutar en modo CLI")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.cli:
        print("Modo CLI aún no implementado. Lanza la GUI por defecto.")
    launch_gui()


if __name__ == "__main__":
    sys.exit(main())
