from pathlib import Path
import pandas as pd

DATA_DIR = Path("../data")

def load_encounters() -> pd.DataFrame:
    return pd.read_parquet(DATA_DIR / "raw" / "encounters.parquet")

def load_anomalies() -> pd.DataFrame:
    p = DATA_DIR / "raw" / "encounter_anomalies.parquet"
    return pd.read_parquet(p) if p.exists() else None
