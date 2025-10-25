from utils import download_retrosheet_data

import datetime
import duckdb
import os


conn = duckdb.connect(":memory:")


class Player:
    def __init__(self, id: str) -> None:
        self.id = id

    def avg(
        self,
        start_date: datetime.date,
        end_date: datetime.date,
        *,
        num_decimal_places: int = 3,
    ) -> float | None:
        """Calculates the player's batting average across all their games between `start_date` and `end_date` (inclusive)."""

        if start_date > end_date:
            raise ValueError("start_date must be before end_date")
        if start_date.year != end_date.year:
            raise ValueError("start_date and end_date must be in the same year")

        if not os.path.exists(f"retrosheet/{start_date.year}"):
            download_retrosheet_data(start_date.year)

        plays_table_name = f"plays{start_date.year}"
        plays_table_exists = conn.execute(f"SELECT * FROM information_schema.tables WHERE table_name = '{plays_table_name}'").fetchall()
        if not plays_table_exists:
            conn.execute(f"CREATE TABLE {plays_table_name} AS SELECT * FROM 'retrosheet/{start_date.year}/{start_date.year}plays.csv'")

        start_date_str = start_date.strftime("%Y/%m/%d")
        end_date_str = end_date.strftime("%Y/%m/%d")

        num_at_bats = conn.execute(f"SELECT COUNT(*) FROM {plays_table_name} WHERE batter = '{self.id}' AND ab = 1 AND strptime(CAST(date AS VARCHAR), '%Y%m%d') BETWEEN DATE '{start_date_str}' AND DATE '{end_date_str}'").fetchone()[0]
        num_hits = conn.execute(f"SELECT COUNT(*) FROM {plays_table_name} WHERE batter = '{self.id}' AND ab = 1 AND (single = 1 OR double = 1 OR triple = 1 OR hr = 1) AND strptime(CAST(date AS VARCHAR), '%Y%m%d') BETWEEN DATE '{start_date_str}' AND DATE '{end_date_str}'").fetchone()[0]
        if num_at_bats == 0:
            return None

        avg = num_hits / num_at_bats

        return round(avg, num_decimal_places)

    def obp(
        self,
        start_date: datetime.date,
        end_date: datetime.date,
        *,
        num_decimal_places: int = 3,
    ) -> float | None:
        """Calculates the player's on-base percentage across all their games between `start_date` and `end_date` (inclusive)."""

        if start_date > end_date:
            raise ValueError("start_date must be before end_date")
        if start_date.year != end_date.year:
            raise ValueError("start_date and end_date must be in the same year")

        if not os.path.exists(f"retrosheet/{start_date.year}"):
            download_retrosheet_data(start_date.year)

        plays_table_name = f"plays{start_date.year}"
        plays_table_exists = conn.execute(f"SELECT * FROM information_schema.tables WHERE table_name = '{plays_table_name}'").fetchall()
        if not plays_table_exists:
            conn.execute(f"CREATE TABLE {plays_table_name} AS SELECT * FROM 'retrosheet/{start_date.year}/{start_date.year}plays.csv'")

        start_date_str = start_date.strftime("%Y/%m/%d")
        end_date_str = end_date.strftime("%Y/%m/%d")

        h_bb_hbp = conn.execute(f"SELECT COUNT(*) FROM {plays_table_name} WHERE batter = '{self.id}' AND (single = 1 OR double = 1 OR triple = 1 OR hr = 1 OR walk = 1 OR iw = 1 OR hbp = 1) AND strptime(CAST(date AS VARCHAR), '%Y%m%d') BETWEEN DATE '{start_date_str}' AND DATE '{end_date_str}'").fetchone()[0]
        ab_bb_hbp_sf = conn.execute(f"SELECT COUNT(*) FROM {plays_table_name} WHERE batter = '{self.id}' AND (ab = 1 OR walk = 1 OR iw = 1 OR hbp = 1 OR sf = 1) AND strptime(CAST(date AS VARCHAR), '%Y%m%d') BETWEEN DATE '{start_date_str}' AND DATE '{end_date_str}'").fetchone()[0]
        if ab_bb_hbp_sf == 0:
            return None

        obp = h_bb_hbp / ab_bb_hbp_sf

        return round(obp, num_decimal_places)

    def era(
        self,
        start_date: datetime.date,
        end_date: datetime.date,
        *,
        num_decimal_places: int = 3,
    ) -> float | None:
        """Calculates the player's earned run average across all their games between `start_date` and `end_date` (inclusive)."""

        if start_date > end_date:
            raise ValueError("start_date must be before end_date")
        if start_date.year != end_date.year:
            raise ValueError("start_date and end_date must be in the same year")

        if not os.path.exists(f"retrosheet/{start_date.year}"):
            download_retrosheet_data(start_date.year)

        plays_table_name = f"plays{start_date.year}"
        plays_table_exists = conn.execute(f"SELECT * FROM information_schema.tables WHERE table_name = '{plays_table_name}'").fetchall()
        if not plays_table_exists:
            conn.execute(f"CREATE TABLE {plays_table_name} AS SELECT * FROM 'retrosheet/{start_date.year}/{start_date.year}plays.csv'")

        start_date_str = start_date.strftime("%Y/%m/%d")
        end_date_str = end_date.strftime("%Y/%m/%d")

        era_query = f"""
        SELECT
            SUM(outs_post - outs_pre) / 3.0 AS ip,
            SUM(
                CASE WHEN prun_b = '{self.id}' AND run_b IS NOT NULL AND ur_b = 0 THEN 1 ELSE 0 END +
                CASE WHEN prun1 = '{self.id}' AND ur1 = 0 THEN 1 ELSE 0 END +
                CASE WHEN prun2 = '{self.id}' AND ur2 = 0 THEN 1 ELSE 0 END +
                CASE WHEN prun3 = '{self.id}' AND ur3 = 0 THEN 1 ELSE 0 END
            ) as runs_allowed
        FROM {plays_table_name}
        WHERE
            pitcher = '{self.id}'
            AND strptime(CAST(date AS VARCHAR), '%Y%m%d') BETWEEN DATE '{start_date_str}' AND DATE '{end_date_str}'
        """
        ip, runs_allowed = conn.execute(era_query).fetchone()
        if ip is None or runs_allowed is None:
            return None

        era = (runs_allowed / ip) * 9

        return round(era, num_decimal_places)
