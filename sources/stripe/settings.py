"""Stripe source — endpoint constants."""

from __future__ import annotations

STRIPE_API_BASE_URL = "https://api.stripe.com/v1"

# Resources extracted by the source.  Order matches the legacy Airbyte streams.
OBJECTS: tuple[str, ...] = ("charges", "customers", "invoices", "refunds")
