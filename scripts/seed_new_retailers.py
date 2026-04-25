"""One-shot insert of the 11 new retailers built in this batch.

Ids 20-36 are reserved for the 17 candidates from the priority list;
6 were skipped during recon (see commit message), so this script only
seeds the 11 that have working scraper files. Rows are inserted with
is_active=False so health_check.py does not alarm on absent data while
the scrape.yml entries remain commented-out.

Idempotent: uses upsert on slug.
"""
import os
import sys
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

SUPABASE_URL = os.environ.get("SUPABASE_URL") or os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY") or os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    print("ERROR: SUPABASE_URL/SUPABASE_KEY not set in env")
    sys.exit(1)

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# Shipping notes flagged "estimate" need spot-checking on the live site
# before being trusted; pattern follows migrations/003_retailers_shipping_config.sql.
RETAILERS = [
    {
        "id": 21,
        "slug": "buckinghorse",
        "name": "Bucking Horse Outpost",
        "website_url": "https://buckinghorseoutpost.com",
        "is_active": False,
        "free_ship_threshold": 199,
        "flat_ship_rate": 14.99,
        "notes": "Free over $199 (estimate - verify)",
    },
    {
        "id": 22,
        "slug": "recoilgunworks",
        "name": "RecoilGunWorks",
        "website_url": "https://recoilgunworks.com",
        "is_active": False,
        "free_ship_threshold": 99,
        "flat_ship_rate": 12.99,
        "notes": "Free over $99 (estimate - verify); some products ship free with 2+ boxes",
    },
    {
        "id": 23,
        "slug": "outdoorlimited",
        "name": "Outdoor Limited",
        "website_url": "https://www.outdoorlimited.com",
        "is_active": False,
        "flat_ship_rate": 12.99,
        "notes": "Flat rate shipping (estimate - verify)",
    },
    {
        "id": 24,
        "slug": "shadowsmith",
        "name": "Shadowsmith Ammo",
        "website_url": "https://www.shadowsmith.net",
        "is_active": False,
        "flat_ship_rate": 14.99,
        "notes": "Flat rate (estimate - verify); shadowsmithammo.com 301-redirects to shadowsmith.net",
    },
    {
        "id": 25,
        "slug": "velocity",
        "name": "Velocity Ammo Sales",
        "website_url": "https://www.velocityammosales.com",
        "is_active": False,
        "free_ship_threshold": 200,
        "flat_ship_rate": 14.99,
        "notes": "Free shipping on orders $200+ (per homepage banner)",
    },
    {
        "id": 26,
        "slug": "gritr",
        "name": "Gritr Sports",
        "website_url": "https://www.gritrsports.com",
        "is_active": False,
        "flat_ship_rate": 9.99,
        "notes": "Flat rate (estimate - verify)",
    },
    {
        "id": 27,
        "slug": "ventura",
        "name": "Ventura Munitions",
        "website_url": "https://www.venturamunitions.com",
        "is_active": False,
        "flat_ship_rate": 14.99,
        "notes": "Flat rate (estimate - verify)",
    },
    {
        "id": 28,
        "slug": "bulkmunitions",
        "name": "Bulk Munitions",
        "website_url": "https://www.bulkmunitions.com",
        "is_active": False,
        "flat_ship_rate": 14.99,
        "notes": "Flat rate (estimate - verify)",
    },
    {
        "id": 29,
        "slug": "gorilla",
        "name": "Gorilla Ammunition",
        "website_url": "https://www.gorillaammo.com",
        "is_active": False,
        "free_ship_threshold": 99,
        "flat_ship_rate": 9.99,
        "notes": "Manufacturer direct; free over $99 (estimate - verify)",
    },
    {
        "id": 30,
        "slug": "georgiaarms",
        "name": "Georgia Arms",
        "website_url": "https://www.georgia-arms.com",
        "is_active": False,
        "flat_ship_rate": 14.99,
        "notes": "Manufacturer direct; flat rate (estimate - verify)",
    },
    {
        "id": 31,
        "slug": "blackbasin",
        "name": "Black Basin",
        "website_url": "https://blackbasin.com",
        "is_active": False,
        "flat_ship_rate": 14.99,
        "notes": "Flat rate (estimate - verify)",
    },
]

# Skipped (no scraper file): South Georgia Outdoors, MidwayUSA,
# American Reloading, Maryland Munitions, Sportsman's Guide, AmmoSquared.
# See commit message for why each was skipped.
#
# Ids are auto-assigned (id=20 was already taken by Optics Planet, so the
# user's "start at 20" target shifts to whatever the sequence yields next).

def main():
    print(f"Upserting {len(RETAILERS)} retailers (auto-assigned ids)...")
    for r in RETAILERS:
        result = supabase.table("retailers").upsert(r, on_conflict="slug").execute()
        if result.data:
            assigned_id = result.data[0].get("id", "?")
            print(f"  id={assigned_id:>3}  {r['slug']:<20}  {r['name']}")
        else:
            print(f"  WARN: upsert returned no data for {r['slug']}")
    print("Done.")

if __name__ == "__main__":
    main()
