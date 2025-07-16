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
        items = [
            Item(
                id=it.get("id") or it.get("item", {}).get("id"),
                title=it.get("title") or it.get("item", {}).get("title", ""),
                quantity=it["quantity"],
                barcode=it.get("barcode"),
                sku=(it.get("seller_sku") or it.get("seller_custom_field") or
                     it.get("item", {}).get("seller_sku") or it.get("item", {}).get("seller_custom_field")),
            )
            for it in data.get("order_items", [])
        ]
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
