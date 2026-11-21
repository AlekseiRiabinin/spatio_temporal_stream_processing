import pandas as pd
from sqlalchemy import create_engine
from pathlib import Path

PARSED_DIR = Path("/home/aleksei/Projects/spatio_temporal_stream_processing/data/athletes/parsed")
DB_URL = "postgresql+psycopg2://cre_user:cre_password@localhost:5436/cre_db"
engine = create_engine(DB_URL)

RENAME = {
    "sample_times_for_all_signals_apart_from_ecg": "wallclock_sec",
}


for csv in sorted(PARSED_DIR.glob("*.csv")):
    df = pd.read_csv(csv).rename(columns=RENAME)
    df.to_sql("athlete_telemetry", engine, schema="public",
              if_exists="append", index=False, chunksize=10000)
    print(f"loaded {csv.stem}: {len(df)} rows")
