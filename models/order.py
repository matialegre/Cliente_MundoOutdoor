from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Dict

@dataclass
class Item:
    id: int
    title: str
    quantity: int
    barcode: str | None = None
    sku: str | None = None
    size: str | None = None
    color: str | None = None
    real_sku: str | None = None  # SKU real resuelto para productos OUT
    item_id: str | None = None   # ID del item en ML
    variation_id: str | None = None  # ID de la variación en ML

@dataclass
class Order:
    id: int
    date_created: str
    buyer: str
    pack_id: int | None = None
    notes: str | None = None
    items: List[Item] = field(default_factory=list)
    shipping_id: int | None = None
    shipping_status: str | None = None
    shipping_substatus: str | None = None

    @classmethod
    def from_api(cls, data: Dict) -> "Order":
        items = []
        for it in data.get("order_items", []):
            # Extraer talla/color si vienen en variation_attributes
            size = color = None
            for attr in it.get("item", {}).get("variation_attributes", []):
                if attr.get("id") in ("SIZE", "TALLE"):
                    size = attr.get("value_name")
                if attr.get("id") == "COLOR":
                    color = attr.get("value_name")
            # Extraer item_id y variation_id
            item_data = it.get("item", {})
            item_id = str(item_data.get("id", "")) if item_data.get("id") else None
            variation_id = str(item_data.get("variation_id", "")) if item_data.get("variation_id") else None
            
            items.append(Item(
                id=it.get("id") or item_data.get("id"),
                title=it.get("title") or item_data.get("title", ""),
                quantity=it["quantity"],
                barcode=it.get("barcode"),
                sku=(it.get("seller_sku") or it.get("seller_custom_field") or
                     item_data.get("seller_sku") or item_data.get("seller_custom_field")),
                size=size,
                color=color,
                item_id=item_id,
                variation_id=variation_id,
                real_sku=None,  # Se resolverá después
            ))
        return cls(
            id=data["id"],
            date_created=data["date_created"],
            buyer=data.get("buyer", {}).get("nickname", ""),
            pack_id=data.get("pack_id"),
            notes=None,  # se completará luego
            items=items,
            shipping_id=data.get("shipping", {}).get("id"),
            shipping_status=data.get("shipping", {}).get("status"),
            shipping_substatus=data.get("shipping", {}).get("substatus"),
        )
