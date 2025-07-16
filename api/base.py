"""Clases y utilidades comunes para los wrappers de API"""
from __future__ import annotations

import requests
from typing import Any

class APIError(Exception):
    """Excepción base para errores de API HTTP"""

    def __init__(self, status_code: int, message: str | None = None, *, response: requests.Response | None = None):
        self.status_code = status_code
        self.message = message or f"Request failed with status {status_code}"
        self.response = response
        super().__init__(self.message)

    @classmethod
    def from_response(cls, resp: requests.Response) -> "APIError":
        try:
            detail: Any = resp.json()
        except ValueError:
            detail = resp.text
        return cls(resp.status_code, str(detail)[:400], response=resp)
