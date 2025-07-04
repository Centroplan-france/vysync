# ===============================
# File: vysync/cli.py
# ===============================
"""Entry‑point: récupère snapshots VCOM & DB, calcule diff, applique, puis récap."""
import argparse
from vysync.adapters.vcom_adapter import fetch_snapshot as vcom_snapshot
from vysync.adapters.supabase_adapter import SupabaseAdapter
from vysync.vcom_client import VCOMAPIClient
from vysync.diff import diff_entities
from vysync.app_logging import init_logger

logger = init_logger(__name__)
from vysync.adapters.yuman_adapter import YumanAdapter

def main():
    parser = argparse.ArgumentParser(description="Sync VCOM ↔ Supabase ↔ Yuman (snapshot/diff)")
    args = parser.parse_args()

    # 0. Init clients
    vc = VCOMAPIClient()
    sb = SupabaseAdapter()
    ya = YumanAdapter(sb)

    # 1. VCOM ➜ DB --------------------------------------------------------
    v_sites, v_equips = vcom_snapshot(vc)
    db_sites = sb.fetch_sites()
    db_equips = sb.fetch_equipments()

    patch_sites = diff_entities(db_sites, v_sites)
    patch_equips = diff_entities(db_equips, v_equips)

    logger.info("[VCOM→DB] Sites Δ +%s ~%s -%s", *map(len, patch_sites))
    logger.info("[VCOM→DB] Equips Δ +%s ~%s -%s", *map(len, patch_equips))

    sb.apply_sites_patch(patch_sites)
    sb.apply_equips_patch(patch_equips)

    # 2. DB ➜ Yuman -------------------------------------------------------
    db_sites = sb.fetch_sites()          # refresh after insert
    db_equips = sb.fetch_equipments()

    ya.apply_sites_patch(db_sites)
    ya.apply_equips_patch(db_equips)

    logger.info("✅ Synchronisation complète terminée")

if __name__ == "__main__":
    main()
