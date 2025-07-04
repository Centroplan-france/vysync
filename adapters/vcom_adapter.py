# ===============================
# File: vysync/adapters/vcom_adapter.py
# ===============================
"""Transforme la sortie de VCOMAPIClient en snapshot ``Site`` / ``Equipment``."""
from __future__ import annotations

from typing import Dict
from vysync.models import Site, Equipment, CAT_INVERTER, CAT_MODULE, CAT_STRING
from vysync.vcom_client import VCOMAPIClient  # réutilise ton client existant
from vysync.app_logging import init_logger

logger = init_logger(__name__)


def fetch_snapshot(vc: VCOMAPIClient) -> tuple[Dict[str, Site], Dict[tuple[str, str], Equipment]]:
    sites: Dict[str, Site] = {}
    equips: Dict[tuple[str, str], Equipment] = {}

    for sys in vc.get_systems():
        key = sys["key"]
        tech = vc.get_technical_data(key)
        det = vc.get_system_details(key)

        site = Site(
            vcom_system_key=key,
            name=sys.get("name") or key,
            latitude=det.get("coordinates", {}).get("latitude"),
            longitude=det.get("coordinates", {}).get("longitude"),
            nominal_power=tech.get("nominalPower"),
            commission_date=det.get("commissionDate"),
            address=det.get("address", {}).get("street"),
        )
        sites[site.key()] = site

        # Modules (on suppose une seule référence)
        panels = tech.get("panels") or []
        if panels:
            p = panels[0]
            mod = Equipment(
                site_key=key,
                category_id=CAT_MODULE,
                eq_type="module",
                vcom_device_id=f"MODULES-{key}",
                name=p.get("model") or "Modules",
                brand=p.get("vendor"),
                model=p.get("model"),
                count=p.get("count"),
            )
            equips[mod.key()] = mod

        # Onduleurs
        for inv in vc.get_inverters(key):
            det_inv = vc.get_inverter_details(key, inv["id"])
            inv_eq = Equipment(
                site_key=key,
                category_id=CAT_INVERTER,
                eq_type="inverter",
                vcom_device_id=inv["id"],
                name=inv.get("name") or inv["id"],
                brand=det_inv.get("vendor"),
                model=det_inv.get("model"),
                serial_number=inv.get("serial"),
            )
            equips[inv_eq.key()] = inv_eq

        # TODO : strings si besoin
    logger.info("[VCOM] snapshot: %s sites, %s equips", len(sites), len(equips))
    return sites, equips
