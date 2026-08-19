import os


from pyiceberg.catalog import load_catalog

"""One-shot check that Flink actually landed match_facts in Iceberg.
Opens demo.match_facts through the REST catalog and Silo (S3 FileIO), not
container disk or Flink stdout. Asserts row count is near 12 * MATCH_COUNT and
that a player's later row has rank-as-of equal to rank after the previous
Iceberg row (win +30 / loss -15), not current Postgres rank. Career-first
bronze/3/0 is printed only; MatchJoin often drops the opening matches so that
row may be absent. Runs from Compose after matches exits.
"""

TIERS = ["bronze", "silver", "gold", "platinum", "diamond"]
PTS_PER_DIV, DIVS_PER_TIER = 100, 4
PTS_PER_TIER = PTS_PER_DIV * DIVS_PER_TIER
WIN_DELTA, LOSS_DELTA = 30, -15


def derive_rank(pts: int) -> tuple[str, int]:
    pts = max(0, min(pts, PTS_PER_TIER * len(TIERS) - 1))
    tier_idx, within = divmod(pts, PTS_PER_TIER)
    return TIERS[tier_idx], DIVS_PER_TIER - within // PTS_PER_DIV


def as_of(pts: int) -> tuple[str, int, int]:
    tier, div = derive_rank(pts)
    return tier, div, pts


def main() -> int:
    expected = 12 * int(os.environ.get("MATCH_COUNT", "400"))
    catalog = load_catalog(
        "lake",
        **{
            "type": "rest",
            "uri": os.environ.get("ICEBERG_REST_URI", "http://localhost:8181"),
            "warehouse": os.environ.get("ICEBERG_WAREHOUSE", "s3://warehouse/"),
            "s3.endpoint": os.environ.get("AWS_S3_ENDPOINT", "http://localhost:9000"),
            "s3.path-style-access": "true",
            "s3.access-key-id": os.environ.get("AWS_ACCESS_KEY_ID", "admin"),
            "s3.secret-access-key": os.environ.get("AWS_SECRET_ACCESS_KEY", "password"),
            "client.region": os.environ.get("AWS_REGION", "us-east-1"),
            "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
        },
    )
    df = catalog.load_table("demo.match_facts").scan().to_pandas()
    n = len(df)
    print(f"match_facts rows: {n}  (expected {expected})")
    if n < expected * 0.95:
        print("FAIL: row count short (checkpoint lag or sink)")
        return 1

    starters = df[
        (df["rank_tier_at_match_start"] == "bronze")
        & (df["rank_division_at_match_start"] == 3)
        & (df["rank_points_at_match_start"] == 0)
    ]
    print(f"career-first rows (bronze/3/0): {len(starters)}")

    df = df.sort_values(["player_id", "started_at"])
    pid = df.groupby("player_id").size().loc[lambda s: s >= 2].index[0]
    rows = df[df["player_id"] == pid]
    first, second = rows.iloc[0], rows.iloc[1]

    t0 = (
        first["rank_tier_at_match_start"],
        int(first["rank_division_at_match_start"]),
        int(first["rank_points_at_match_start"]),
    )
    pts = int(first["rank_points_at_match_start"])
    pts += WIN_DELTA if first["result"] == "win" else LOSS_DELTA
    pts = max(0, pts)
    want = as_of(pts)
    got = (
        second["rank_tier_at_match_start"],
        int(second["rank_division_at_match_start"]),
        int(second["rank_points_at_match_start"]),
    )
    print(f"player {pid}: first={t0[0]}/{t0[1]}/{t0[2]}")
    print(f"player {pid}: second={got[0]}/{got[1]}/{got[2]}  (want {want[0]}/{want[1]}/{want[2]})")
    if got != want:
        print("FAIL: second match as-of is not rank after previous Iceberg row")
        return 1

    print("PASS")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())