"""
schemas.py — Dataclasses and JSON schema definitions for all RetailPulse events.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import Any, Dict, List, Optional


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def _now_iso() -> str:
    return datetime.utcnow().isoformat() + "Z"


def _new_uuid() -> str:
    return str(uuid.uuid4())


# ---------------------------------------------------------------------------
# Clickstream
# ---------------------------------------------------------------------------

CLICKSTREAM_EVENT_TYPES = (
    "page_view",
    "product_view",
    "add_to_cart",
    "remove_from_cart",
    "search",
    "checkout_start",
    "checkout_complete",
)

DEVICE_TYPES = ("mobile", "desktop", "tablet")


@dataclass
class ClickstreamEvent:
    event_id: str = field(default_factory=_new_uuid)
    user_id: str = ""
    session_id: str = field(default_factory=_new_uuid)
    timestamp: str = field(default_factory=_now_iso)
    event_type: str = "page_view"           # one of CLICKSTREAM_EVENT_TYPES
    page_url: str = ""
    product_id: Optional[str] = None
    product_category: str = ""
    search_query: Optional[str] = None
    device_type: str = "mobile"             # one of DEVICE_TYPES
    browser: str = ""
    country: str = ""
    city: str = ""
    referrer: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


CLICKSTREAM_JSON_SCHEMA: Dict[str, Any] = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "title": "ClickstreamEvent",
    "type": "object",
    "required": [
        "event_id", "user_id", "session_id", "timestamp", "event_type",
        "page_url", "product_category", "device_type", "browser",
        "country", "city", "referrer",
    ],
    "properties": {
        "event_id":         {"type": "string", "format": "uuid"},
        "user_id":          {"type": "string"},
        "session_id":       {"type": "string", "format": "uuid"},
        "timestamp":        {"type": "string", "format": "date-time"},
        "event_type":       {"type": "string", "enum": list(CLICKSTREAM_EVENT_TYPES)},
        "page_url":         {"type": "string"},
        "product_id":       {"type": ["string", "null"]},
        "product_category": {"type": "string"},
        "search_query":     {"type": ["string", "null"]},
        "device_type":      {"type": "string", "enum": list(DEVICE_TYPES)},
        "browser":          {"type": "string"},
        "country":          {"type": "string"},
        "city":             {"type": "string"},
        "referrer":         {"type": "string"},
    },
    "additionalProperties": False,
}


# ---------------------------------------------------------------------------
# Orders
# ---------------------------------------------------------------------------

ORDER_EVENT_TYPES = (
    "order_created",
    "order_shipped",
    "order_delivered",
    "order_cancelled",
    "order_returned",
)

PAYMENT_METHODS = ("credit_card", "debit_card", "paypal", "bank_transfer", "crypto")

SHIPPING_METHODS = ("standard", "express", "overnight")


@dataclass
class OrderItem:
    product_id: str = ""
    product_name: str = ""
    category: str = ""
    quantity: int = 1
    unit_price: float = 0.0
    discount_pct: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class ShippingAddress:
    country: str = ""
    city: str = ""
    zip: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class OrderEvent:
    order_id: str = field(default_factory=_new_uuid)
    user_id: str = ""
    event_type: str = "order_created"       # one of ORDER_EVENT_TYPES
    timestamp: str = field(default_factory=_now_iso)
    items: List[OrderItem] = field(default_factory=list)
    subtotal: float = 0.0
    discount_amount: float = 0.0
    tax_amount: float = 0.0
    total_amount: float = 0.0
    payment_method: str = "credit_card"     # one of PAYMENT_METHODS
    shipping_method: str = "standard"       # one of SHIPPING_METHODS
    shipping_address: ShippingAddress = field(default_factory=ShippingAddress)
    coupon_code: Optional[str] = None
    status: str = "created"

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        return d


ORDER_ITEM_JSON_SCHEMA: Dict[str, Any] = {
    "type": "object",
    "required": ["product_id", "product_name", "category", "quantity", "unit_price", "discount_pct"],
    "properties": {
        "product_id":   {"type": "string"},
        "product_name": {"type": "string"},
        "category":     {"type": "string"},
        "quantity":     {"type": "integer", "minimum": 1},
        "unit_price":   {"type": "number", "minimum": 0},
        "discount_pct": {"type": "number", "minimum": 0, "maximum": 100},
    },
}

ORDER_JSON_SCHEMA: Dict[str, Any] = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "title": "OrderEvent",
    "type": "object",
    "required": [
        "order_id", "user_id", "event_type", "timestamp", "items",
        "subtotal", "discount_amount", "tax_amount", "total_amount",
        "payment_method", "shipping_method", "shipping_address", "status",
    ],
    "properties": {
        "order_id":        {"type": "string", "format": "uuid"},
        "user_id":         {"type": "string"},
        "event_type":      {"type": "string", "enum": list(ORDER_EVENT_TYPES)},
        "timestamp":       {"type": "string", "format": "date-time"},
        "items":           {"type": "array", "items": ORDER_ITEM_JSON_SCHEMA, "minItems": 1},
        "subtotal":        {"type": "number", "minimum": 0},
        "discount_amount": {"type": "number", "minimum": 0},
        "tax_amount":      {"type": "number", "minimum": 0},
        "total_amount":    {"type": "number", "minimum": 0},
        "payment_method":  {"type": "string", "enum": list(PAYMENT_METHODS)},
        "shipping_method": {"type": "string", "enum": list(SHIPPING_METHODS)},
        "shipping_address": {
            "type": "object",
            "required": ["country", "city", "zip"],
            "properties": {
                "country": {"type": "string"},
                "city":    {"type": "string"},
                "zip":     {"type": "string"},
            },
        },
        "coupon_code":  {"type": ["string", "null"]},
        "status":       {"type": "string"},
    },
    "additionalProperties": False,
}


# ---------------------------------------------------------------------------
# Inventory
# ---------------------------------------------------------------------------

INVENTORY_EVENT_TYPES = ("restock", "sale", "adjustment", "damage", "return")


@dataclass
class InventoryEvent:
    inventory_id: str = field(default_factory=_new_uuid)
    product_id: str = ""
    product_name: str = ""
    category: str = ""
    sku: str = ""
    timestamp: str = field(default_factory=_now_iso)
    event_type: str = "sale"               # one of INVENTORY_EVENT_TYPES
    quantity_change: int = 0               # positive = in, negative = out
    quantity_before: int = 0
    quantity_after: int = 0
    warehouse_id: str = ""
    supplier_id: Optional[str] = None
    unit_cost: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


INVENTORY_JSON_SCHEMA: Dict[str, Any] = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "title": "InventoryEvent",
    "type": "object",
    "required": [
        "inventory_id", "product_id", "product_name", "category", "sku",
        "timestamp", "event_type", "quantity_change",
        "quantity_before", "quantity_after", "warehouse_id", "unit_cost",
    ],
    "properties": {
        "inventory_id":    {"type": "string", "format": "uuid"},
        "product_id":      {"type": "string"},
        "product_name":    {"type": "string"},
        "category":        {"type": "string"},
        "sku":             {"type": "string"},
        "timestamp":       {"type": "string", "format": "date-time"},
        "event_type":      {"type": "string", "enum": list(INVENTORY_EVENT_TYPES)},
        "quantity_change": {"type": "integer"},
        "quantity_before": {"type": "integer", "minimum": 0},
        "quantity_after":  {"type": "integer", "minimum": 0},
        "warehouse_id":    {"type": "string"},
        "supplier_id":     {"type": ["string", "null"]},
        "unit_cost":       {"type": "number", "minimum": 0},
    },
    "additionalProperties": False,
}
