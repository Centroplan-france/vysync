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

def main():
    parser = argparse.ArgumentParser(description="Sync VCOM → Supabase (snapshot/diff)")
    args = parser.parse_args()

    vc = VCOMAPIClient()
    sb = SupabaseAdapter()

    # 1. snapshots
    v_sites, v_equips = vcom_snapshot(vc)
    db_sites = sb.fetch_sites()
    db_equips = sb.fetch_equipments()

    # 2. diff
    patch_sites = diff_entities(db_sites, v_sites)
    patch_equips = diff_entities(db_equips, v_equips)

    logger.info("Sites Δ: +%s / ~%s / -%s", len(patch_sites.add), len(patch_sites.update), len(patch_sites.delete))
    logger.info("Equips Δ: +%s / ~%s / -%s", len(patch_equips.add), len(patch_equips.update), len(patch_equips.delete))

    # 3. apply patches
    sb.apply_sites_patch(patch_sites)
    sb.apply_equips_patch(patch_equips)

    logger.info("✅ DB updated successfully")

if __name__ == "__main__":
    main()
