# Cliente Matias Refactor

Este proyecto es una refactorización del script monolítico `Cliente1.0.py` hacia una arquitectura modular.

* GUI basada en **ttkbootstrap** en lugar de Tkinter puro.
* Separación de responsabilidades (GUI, lógica de negocio, acceso a APIs, impresión, etc.).
* Carga de secretos mediante variables de entorno (`python-dotenv`).
* Preparado para pruebas unitarias con **pytest**.

## Estructura

```
app/
  main.py            # Punto de entrada
api/
  ml_api.py          # API MercadoLibre
  dragonfish_api.py  # API Dragonfish
models/
  order.py           # Dataclass Order
  item.py            # Dataclass Item
services/
  picker_service.py  # Orquesta procesos de pickeo
printing/
  zpl_printer.py     # Envío de etiquetas a impresora Zebra
gui/
  app_gui.py         # Interfaz gráfica ttkbootstrap
utils/
  config.py          # Carga de configuración y secretos
  logger.py          # Configuración de logging
.env.example         # Variables de entorno de ejemplo
```

## Instalación

```bash
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

## Ejecución

```bash
python -m app
```

## Próximos pasos
1. Completar los métodos pendientes en los módulos `api` y `services`.
2. Agregar pruebas unitarias en `tests/`.
