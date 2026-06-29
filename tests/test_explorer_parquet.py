"""
Validates docs/data/accessions_explorer.parquet after each rebuild.
Runs fast (DuckDB queries against the committed local file).
"""

import pytest
import duckdb
from pathlib import Path

PARQUET = Path(__file__).parent.parent / "docs" / "data" / "accessions_explorer.parquet"
EXPECTED_COLS = {
    "personnel_action_effective_date_yyyymm",
    "agency",
    "agency_subelement",
    "occupational_series",
    "occupational_series_code",
    "age_bracket",
    "education_level",
    "length_of_service_years",
    "accession_category",
    "pay_plan",
    "grade",
    "appointment_type",
    "supervisory_status",
    "veteran_indicator",
    "count",
}


@pytest.fixture(scope="module")
def con():
    return duckdb.connect()


@pytest.fixture(scope="module")
def path():
    assert PARQUET.exists(), f"Parquet not found: {PARQUET}"
    return str(PARQUET)


def q(con, path, sql):
    return con.execute(sql.format(p=f"read_parquet('{path}')")).fetchone()


def test_parquet_exists():
    assert PARQUET.exists()
    assert PARQUET.stat().st_size > 10_000_000, "Parquet too small (<10 MB)"


def test_columns(con, path):
    cols = {
        row[0]
        for row in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{path}')").fetchall()
    }
    missing = EXPECTED_COLS - cols
    assert not missing, f"Missing columns: {missing}"


def test_row_count(con, path):
    n = q(con, path, "SELECT COUNT(*) FROM {p}")[0]
    assert n > 5_000_000, f"Too few rows: {n:,}"


def test_date_range(con, path):
    lo, hi = con.execute(
        f"SELECT MIN(personnel_action_effective_date_yyyymm), "
        f"MAX(personnel_action_effective_date_yyyymm) "
        f"FROM read_parquet('{path}')"
    ).fetchone()
    assert lo <= "200512", f"Unexpected earliest date: {lo}"
    assert hi >= "202601", f"Data ends too early: {hi}"


def test_no_null_dates(con, path):
    nulls = q(
        con, path,
        "SELECT COUNT(*) FROM {p} WHERE personnel_action_effective_date_yyyymm IS NULL"
    )[0]
    assert nulls == 0, f"{nulls:,} rows have null effective date"


def test_count_column_castable(con, path):
    bad = q(
        con, path,
        "SELECT COUNT(*) FROM {p} WHERE TRY_CAST(\"count\" AS BIGINT) IS NULL AND \"count\" IS NOT NULL"
    )[0]
    assert bad == 0, f"{bad:,} rows have non-numeric count values"


def test_major_agencies_present(con, path):
    agencies = {
        row[0]
        for row in con.execute(f"SELECT DISTINCT agency FROM read_parquet('{path}')").fetchall()
    }
    for expected in ["DEPARTMENT OF DEFENSE", "DEPARTMENT OF VETERANS AFFAIRS"]:
        assert expected in agencies, f"Expected agency not found: {expected}"


def test_accession_categories(con, path):
    cats = q(con, path, "SELECT COUNT(DISTINCT accession_category) FROM {p}")[0]
    assert cats >= 3, f"Too few accession categories: {cats}"


def test_new_columns_populated(con, path):
    for col in ("pay_plan", "grade", "appointment_type", "supervisory_status", "veteran_indicator"):
        n = q(con, path, f"SELECT COUNT(*) FROM {{p}} WHERE {col} IS NOT NULL")[0]
        assert n > 0, f"Column {col} is entirely NULL"


def test_distinct_agencies(con, path):
    n = q(con, path, "SELECT COUNT(DISTINCT agency) FROM {p}")[0]
    assert n >= 50, f"Too few distinct agencies: {n}"


def test_distinct_series(con, path):
    n = q(con, path, "SELECT COUNT(DISTINCT occupational_series_code) FROM {p}")[0]
    assert n >= 200, f"Too few distinct series: {n}"
