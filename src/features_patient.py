import pandas as pd

def build_patient_signals(encounters: pd.DataFrame, as_of_date: str) -> pd.DataFrame:
    # filter history window, e.g. 180 days
    as_of = pd.to_datetime(as_of_date)
    lookback = as_of - pd.Timedelta(days=180)
    hist = encounters[(encounters["start_time"] >= lookback) &
                      (encounters["start_time"] < as_of)].copy()

    # example signals
    g = hist.groupby("patient_id")
    df = pd.DataFrame(index=g.size().index)
    df["encounters_180d"]      = g.size()
    df["ed_visits_180d"]       = g["encounter_type"].apply(lambda s: (s=="ED").sum())
    df["inpatient_stays_180d"] = g["encounter_type"].apply(lambda s: (s=="INPATIENT").sum())
    df["mean_los_days_180d"]   = g["length_of_stay_days"].mean()

    df = df.reset_index().rename(columns={"patient_id": "patient_id"})
    df["as_of_date"] = as_of.normalize()
    return df[["patient_id","as_of_date",
               "encounters_180d","ed_visits_180d",
               "inpatient_stays_180d","mean_los_days_180d"]]
