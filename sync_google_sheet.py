import pandas as pd
import hashlib
import json
import os
from sqlalchemy import create_engine, text
from datetime import datetime
import ssl
from urllib.parse import quote_plus

ssl._create_default_https_context = ssl._create_unverified_context

SHEET_URL = "https://docs.google.com/spreadsheets/d/1cV7LJhyZV6nCFf2TlvLu-99w78udEse8fSq8MS-nxd8/export?format=csv&gid=692708537"

DB_HOST = "aws-1-ap-south-1.pooler.supabase.com"
DB_PORT = 5432
DB_NAME = "postgres"
DB_USER = "postgres.aisszpvkhderfukystks"
DB_PASSWORD = quote_plus(os.getenv("DB_PASSWORD"))

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DATABASE_URL)

def clean_int(val):
    """
    Convert numeric-looking values to int.
    Return None for blanks, placeholders, or invalid values.
    """
    if pd.isna(val):
        return None

    val = str(val).strip()

    if val in ("", "-", "--", "_", "N/A", "NA"):
        return None

    try:
        return int(val.replace(",", ""))
    except ValueError:
        return None



def clean_float(val):
    if pd.isna(val):
        return None

    val = str(val).strip()

    if val in ("", "-", "--", "_", "N/A", "NA"):
        return None

    try:
        return float(val.replace("$", "").replace(",", ""))
    except ValueError:
        return None




def generate_row_hash(row: dict) -> str:
    """
    Create a SHA256 hash of a row to detect changes
    """
    row_string = json.dumps(row, sort_keys=True, default=str)
    return hashlib.sha256(row_string.encode("utf-8")).hexdigest()

def run_etl():
    inserted = 0
    updated = 0

    # Read Google Sheet
    print("Reading Google Sheet...")
    df = pd.read_csv(SHEET_URL)

    # Clean column names (important)
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
        .str.replace(">", "gt")
        .str.replace("<", "lt")
    )
    print(df.columns.tolist())

    # Convert to list of dicts (JSON)
    records = df.to_dict(orient="records")
    # print(records[0],"records")
    # Generate row_hash for each row
    for r in records:
        r["usd_amount"] = clean_float(r.get("usd"))
        r["individuals"] = clean_int(r.get("individuals"))
        r["family"] = clean_int(r.get("family"))

        r["male_above_18"] = clean_int(r.get("male_gt18"))
        r["female_above_18"] = clean_int(r.get("female_gt18"))
        r["male_below_18"] = clean_int(r.get("male_lt18"))
        r["female_below_18"] = clean_int(r.get("female_lt18"))

    # VERY IMPORTANT: ensure keys exist even if None
        for k in [
            "usd_amount",
            "individuals",
            "family",
            "male_above_18",
            "female_above_18",
            "male_below_18",
            "female_below_18",]:
            r.setdefault(k, None)

        r["row_hash"] = generate_row_hash(r)


    # UPSERT into Supabase
    with engine.begin() as conn:
        for r in records:
            query = text("""
               INSERT INTO aid_metrics (
    serial_number,
    err_code,
    err_name,
    project_status,
    project_donor,
    state,
    usd_amount,
    individuals,
    family,
    male_above_18,
    female_above_18,
    male_below_18,
    female_below_18,
    row_hash,
    updated_at
)
VALUES (
    :serial_number,
    :err_code,
    :err_name,
    :project_status,
    :project_donor,
    :state,
    :usd_amount,
    :individuals,
    :family,
    :male_above_18,
    :female_above_18,
    :male_below_18,
    :female_below_18,
    :row_hash,
    NOW()
)
ON CONFLICT (row_hash)
DO UPDATE SET
    updated_at = NOW();
            """)

            result = conn.execute(query, r)
            # print(result,"result")
            if result.rowcount == 1:
                inserted += 1
            else:
                updated += 1

    # Logging
    print(f"Rows inserted: {inserted}")
    print(f"Rows updated: {updated}")

if __name__ == "__main__":
    try:
        run_etl()
        print("ETL completed successfully")
    except Exception as e:
        print("ETL failed:", str(e))
        raise
