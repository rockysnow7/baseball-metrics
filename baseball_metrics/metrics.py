from collections import OrderedDict
from enum import Enum
from functools import wraps
from .utils import download_retrosheet_data

import datetime
import duckdb
import os


conn = duckdb.connect(":memory:")


class Handedness(Enum):
    LEFT = "L"
    RIGHT = "R"
    BOTH = "B"


class Player:
    _cache: OrderedDict[tuple[str, str, datetime.date, datetime.date], float | None] = OrderedDict()
    _cache_max_size: int = 1000

    @staticmethod
    def _cached(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            key = (self.id, func.__name__, args, tuple(sorted(kwargs.items())))
            cache = Player._cache

            if key in cache:
                cache.move_to_end(key)
                return cache[key]

            result = func(self, *args, **kwargs)
            cache[key] = result
            cache.move_to_end(key)

            if len(cache) > Player._cache_max_size:
                cache.popitem(last=False)

            return result
        return wrapper

    def __init__(self, id: str) -> None:
        self.id = id

    @_cached
    def avg(
        self,
        start_date: datetime.date,
        end_date: datetime.date,
        *,
        num_decimal_places: int = 3,
        min_at_bats: int = 20,
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
        if num_at_bats < min_at_bats:
            return None

        avg = num_hits / num_at_bats

        return round(avg, num_decimal_places)

    @_cached
    def obp(
        self,
        start_date: datetime.date,
        end_date: datetime.date,
        *,
        num_decimal_places: int = 3,
        min_denominator: int = 20,
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

        h_bb_hbp = conn.execute(f"SELECT COUNT(*) FROM {plays_table_name} WHERE batter = '{self.id}' AND (single = 1 OR double = 1 OR triple = 1 OR hr = 1 OR walk = 1 OR hbp = 1) AND strptime(CAST(date AS VARCHAR), '%Y%m%d') BETWEEN DATE '{start_date_str}' AND DATE '{end_date_str}'").fetchone()[0]
        ab_bb_hbp_sf = conn.execute(f"SELECT COUNT(*) FROM {plays_table_name} WHERE batter = '{self.id}' AND (ab = 1 OR walk = 1 OR hbp = 1 OR sf = 1) AND strptime(CAST(date AS VARCHAR), '%Y%m%d') BETWEEN DATE '{start_date_str}' AND DATE '{end_date_str}'").fetchone()[0]
        if ab_bb_hbp_sf < min_denominator:
            return None

        obp = h_bb_hbp / ab_bb_hbp_sf

        return round(obp, num_decimal_places)

    @_cached
    def era(
        self,
        start_date: datetime.date,
        end_date: datetime.date,
        *,
        num_decimal_places: int = 3,
        min_ip: int = 20,
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
        if ip < min_ip:
            return None

        era = (runs_allowed / ip) * 9

        return round(era, num_decimal_places)

    @_cached
    def bat_hand(self, year: int) -> Handedness | None:
        """Returns the player's batting hand."""

        if not os.path.exists(f"retrosheet/{year}"):
            download_retrosheet_data(year)

        allplayers_table_name = f"allplayers{year}"
        allplayers_table_exists = conn.execute(f"SELECT * FROM information_schema.tables WHERE table_name = '{allplayers_table_name}'").fetchall()
        if not allplayers_table_exists:
            conn.execute(f"CREATE TABLE {allplayers_table_name} AS SELECT * FROM 'retrosheet/{year}/{year}allplayers.csv'")

        bat_hand = conn.execute(f"SELECT bat FROM {allplayers_table_name} WHERE id = '{self.id}'").fetchone()[0]
        if bat_hand is None:
            return None
        return Handedness(bat_hand)

    @_cached
    def throw_hand(self, year: int) -> Handedness | None:
        """Returns the player's throwing hand."""

        if not os.path.exists(f"retrosheet/{year}"):
            download_retrosheet_data(year)

        allplayers_table_name = f"allplayers{year}"
        allplayers_table_exists = conn.execute(f"SELECT * FROM information_schema.tables WHERE table_name = '{allplayers_table_name}'").fetchall()
        if not allplayers_table_exists:
            conn.execute(f"CREATE TABLE {allplayers_table_name} AS SELECT * FROM 'retrosheet/{year}/{year}allplayers.csv'")

        throw_hand = conn.execute(f"SELECT throw FROM {allplayers_table_name} WHERE id = '{self.id}'").fetchone()[0]
        if throw_hand is None:
            return None
        return Handedness(throw_hand)

    @_cached
    def k_pct_batting(
        self,
        start_date: datetime.date,
        end_date: datetime.date,
        *,
        num_decimal_places: int = 3,
        min_plate_appearances: int = 20,
    ) -> float | None:
        """Calculates the player's strikeout percentage across all their games as a batter between `start_date` and `end_date` (inclusive)."""

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

        k_pct_query = f"""
        SELECT
            COUNT(*) AS plate_appearances,
            SUM(k) AS k
        FROM {plays_table_name}
        WHERE
            batter = '{self.id}'
            AND pa = 1
            AND strptime(CAST(date AS VARCHAR), '%Y%m%d') BETWEEN DATE '{start_date_str}' AND DATE '{end_date_str}'
        """
        plate_appearances, k = conn.execute(k_pct_query).fetchone()
        if plate_appearances is None or k is None:
            return None
        if plate_appearances < min_plate_appearances:
            return None

        k_pct = k / plate_appearances

        return round(k_pct, num_decimal_places)

    @_cached
    def k_pct_pitching(
        self,
        start_date: datetime.date,
        end_date: datetime.date,
        *,
        num_decimal_places: int = 3,
        min_plate_appearances: int = 20,
    ) -> float | None:
        """Calculates the player's strikeout percentage across all their games as a pitcher between `start_date` and `end_date` (inclusive)."""

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

        k_pct_query = f"""
        SELECT
            COUNT(*) AS plate_appearances,
            SUM(k) AS k
        FROM {plays_table_name}
        WHERE
            pitcher = '{self.id}'
            AND pa = 1
            AND strptime(CAST(date AS VARCHAR), '%Y%m%d') BETWEEN DATE '{start_date_str}' AND DATE '{end_date_str}'
        """
        plate_appearances, k = conn.execute(k_pct_query).fetchone()
        if plate_appearances is None or k is None:
            return None
        if plate_appearances < min_plate_appearances:
            return None

        k_pct = k / plate_appearances

        return round(k_pct, num_decimal_places)

    @_cached
    def bb_pct_batting(
        self,
        start_date: datetime.date,
        end_date: datetime.date,
        *,
        num_decimal_places: int = 3,
        min_plate_appearances: int = 20,
    ) -> float | None:
        """Calculates the player's walk percentage across all their games as a batter between `start_date` and `end_date` (inclusive)."""

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

        bb_pct_query = f"""
        SELECT
            COUNT(*) AS plate_appearances,
            SUM(walk) AS walks
        FROM {plays_table_name}
        WHERE
            batter = '{self.id}'
            AND pa = 1
            AND strptime(CAST(date AS VARCHAR), '%Y%m%d') BETWEEN DATE '{start_date_str}' AND DATE '{end_date_str}'
        """
        plate_appearances, walks = conn.execute(bb_pct_query).fetchone()
        if plate_appearances is None or walks is None:
            return None
        if plate_appearances < min_plate_appearances:
            return None

        bb_pct = walks / plate_appearances

        return round(bb_pct, num_decimal_places)

    @_cached
    def bb_pct_pitching(
        self,
        start_date: datetime.date,
        end_date: datetime.date,
        *,
        num_decimal_places: int = 3,
        min_plate_appearances: int = 20,
    ) -> float | None:
        """Calculates the player's walk percentage across all their games as a pitcher between `start_date` and `end_date` (inclusive)."""

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

        bb_pct_query = f"""
        SELECT
            COUNT(*) AS plate_appearances,
            SUM(walk) AS walks
        FROM {plays_table_name}
        WHERE
            pitcher = '{self.id}'
            AND pa = 1
            AND strptime(CAST(date AS VARCHAR), '%Y%m%d') BETWEEN DATE '{start_date_str}' AND DATE '{end_date_str}'
        """
        plate_appearances, walks = conn.execute(bb_pct_query).fetchone()
        if plate_appearances is None or walks is None:
            return None
        if plate_appearances < min_plate_appearances:
            return None

        bb_pct = walks / plate_appearances

        return round(bb_pct, num_decimal_places)

    @_cached
    def slg(
        self,
        start_date: datetime.date,
        end_date: datetime.date,
        *,
        num_decimal_places: int = 3,
        min_at_bats: int = 20,
    ) -> float | None:
        """Calculates the player's slugging percentage across all their games between `start_date` and `end_date` (inclusive)."""

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

        slg_query = f"""
        SELECT
            COUNT(*) AS at_bats,
            SUM(single) AS singles,
            SUM(double) AS doubles,
            SUM(triple) AS triples,
            SUM(hr) AS home_runs
        FROM {plays_table_name}
        WHERE
            batter = '{self.id}'
            AND ab = 1
            AND strptime(CAST(date AS VARCHAR), '%Y%m%d') BETWEEN DATE '{start_date_str}' AND DATE '{end_date_str}'
        """
        at_bats, singles, doubles, triples, home_runs = conn.execute(slg_query).fetchone()
        if at_bats is None or singles is None or doubles is None or triples is None or home_runs is None:
            return None
        if at_bats < min_at_bats:
            return None

        slg = (singles + doubles * 2 + triples * 3 + home_runs * 4) / at_bats

        return round(slg, num_decimal_places)

    @_cached
    def iso(
        self,
        start_date: datetime.date,
        end_date: datetime.date,
        *,
        num_decimal_places: int = 3,
        min_at_bats: int = 20,
    ) -> float | None:
        """Calculates the player's isolated power across all their games between `start_date` and `end_date` (inclusive)."""

        slg = self.slg(start_date, end_date, num_decimal_places=num_decimal_places + 2, min_at_bats=min_at_bats)
        avg = self.avg(start_date, end_date, num_decimal_places=num_decimal_places + 2, min_at_bats=min_at_bats)
        if slg is None or avg is None:
            return None

        iso_ = slg - avg

        return round(iso_, num_decimal_places)

    @_cached
    def gb_pct_pitching(
        self,
        start_date: datetime.date,
        end_date: datetime.date,
        *,
        num_decimal_places: int = 3,
        min_at_bats: int = 20,
    ) -> float | None:
        """Calculates the player's ground ball percentage (ground balls / balls in play) across all their games as a pitcher between `start_date` and `end_date` (inclusive)."""

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

        gb_pct_query = f"""
        SELECT
            COUNT(*) AS balls_in_play,
            SUM(ground) AS ground_balls
        FROM {plays_table_name}
        WHERE
            pitcher = '{self.id}'
            AND bip = 1
            AND strptime(CAST(date AS VARCHAR), '%Y%m%d') BETWEEN DATE '{start_date_str}' AND DATE '{end_date_str}'
        """
        balls_in_play, ground_balls = conn.execute(gb_pct_query).fetchone()
        if balls_in_play is None or ground_balls is None:
            return None
        if balls_in_play < min_at_bats:
            return None

        gb_pct = ground_balls / balls_in_play

        return round(gb_pct, num_decimal_places)
