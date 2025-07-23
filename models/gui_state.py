from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Tuple

@dataclass
class GuiState:
    """Estado compartido entre los hilos y la GUI.

    Almacena filtros seleccionados por el usuario, listas de pedidos visibles y
    procesados y una cola de mensajes para mostrar en la interfaz.
    """

    filtros: Dict[str, Any] = field(default_factory=dict)
    visibles: List[Any] = field(default_factory=list)
    procesados: List[Any] = field(default_factory=list)
    mensajes: List[Tuple[str, str]] = field(default_factory=list)
