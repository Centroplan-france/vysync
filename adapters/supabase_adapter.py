# ===============================
# File: vysync/adapters/supabase_adapter.py
# ===============================
"""Accès Supabase encapsulé → snapshot + patch apply.
Toutes les requêtes passent par cette classe afin de garder un seul point
pour la journalisation / debug.
"""
from __future__ import annotations

import os
from typing import Dict, List
from supabase import create_client, Client as SupabaseClient

from vysync.app_logging import init_logger
from vysync.models import Site, Equipment, Client, CAT_INVERTER, CAT_MODULE, CAT_STRING

logger = init_logger(__name__)

SITE_TABLE = "sites_mapping"
EQUIP_TABLE = "equipments_mapping"
CLIENT_TABLE = "clients_mapping"


class SupabaseAdapter:
    """Thin wrapper around supabase‑py allowing snapshot/diff style."""

    def __init__(self):
        url = os.getenv("SUPABASE_URL")
        key = os.getenv("SUPABASE_SERVICE_KEY")
        if not url or not key:
            raise EnvironmentError("SUPABASE_URL or SUPABASE_SERVICE_KEY missing")
        self.sb: SupabaseClient = create_client(url, key)

    # ---------------------------------------------------------------------
    # SNAPSHOTS
    # ---------------------------------------------------------------------
    def fetch_sites(self) -> Dict[str, Site]:
        rows = self.sb.table(SITE_TABLE).select("*").execute().data or []
        out: Dict[str, Site] = {}
        for r in rows:
            if not r.get("vcom_system_key"):
                continue  # hors périmètre
            out[r["vcom_system_key"]] = Site(
                vcom_system_key=r["vcom_system_key"],
                name=r.get("name") or r["vcom_system_key"],
                latitude=r.get("latitude"),
                longitude=r.get("longitude"),
                nominal_power=r.get("nominal_power"),
                commission_date=r.get("commission_date"),
                address=r.get("address"),
                yuman_site_id=r.get("yuman_site_id"),
            )
        logger.debug("[SB] fetched %s sites", len(out))
        return out

    def fetch_equipments(self) -> Dict[tuple[str, str], Equipment]:
        rows = (
            self.sb.table(EQUIP_TABLE)
            .select("*")
            .in_("category_id", [CAT_INVERTER, CAT_MODULE, CAT_STRING])
            .execute()
            .data
            or []
        )
        out: Dict[tuple[str, str], Equipment] = {}
        for r in rows:
            k = (r["vcom_system_key"], r["vcom_device_id"])
            out[k] = Equipment(
                site_key=r["vcom_system_key"],
                category_id=r["category_id"],
                eq_type=r["eq_type"],
                vcom_device_id=r["vcom_device_id"],
                name=r["name"],
                brand=r.get("brand"),
                model=r.get("model"),
                serial_number=r.get("serial_number"),
                count=r.get("count"),
                parent_vcom_id=r.get("parent_vcom_id"),
                yuman_material_id=r.get("yuman_material_id"),
            )
        logger.debug("[SB] fetched %s equipments", len(out))
        return out

    # ---------------------------------------------------------------------
    # APPLY PATCHES (simplifié pour Add/Update, ignore Delete par sécurité)
    # ---------------------------------------------------------------------
    def apply_sites_patch(self, patch):
        for s in patch.add:
            logger.debug("[SB] INSERT site %s", s.key())
            self.sb.table(SITE_TABLE).insert([s.to_dict()]).execute()
        for old, new in patch.update:
            logger.debug("[SB] UPDATE site %s", new.key())
            self.sb.table(SITE_TABLE).update(new.to_dict()).eq("vcom_system_key", new.key()).execute()

    def apply_equips_patch(self, patch):
        for e in patch.add:
            logger.debug("[SB] INSERT equip %s", e.key())
            self.sb.table(EQUIP_TABLE).insert([e.to_dict()]).execute()
        for old, new in patch.update:
            logger.debug("[SB] UPDATE equip %s", new.key())
            self.sb.table(EQUIP_TABLE).update(new.to_dict()).eq("vcom_device_id", new.vcom_device_id).eq("vcom_system_key", new.site_key).execute()
