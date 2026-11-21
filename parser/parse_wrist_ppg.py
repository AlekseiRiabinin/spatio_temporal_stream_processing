import wfdb
import pandas as pd
from pathlib import Path

DATA_DIR = Path("/home/aleksei/Projects/spatio_temporal_stream_processing/data/athletes/wrist-ppg-during-exercise-1.0.0")
OUT_DIR = Path("/home/aleksei/Projects/spatio_temporal_stream_processing/data/athletes/parsed")
OUT_DIR.mkdir(parents=True, exist_ok=True)

records = (DATA_DIR / "RECORDS").read_text().strip().splitlines()

for rec_name in records:
    rec_path = str(DATA_DIR / rec_name)
    try:
        record = wfdb.rdrecord(rec_path)
        ann = wfdb.rdann(rec_path, "atr")
    except Exception as e:
        print(f"skip {rec_name}: {e}")
        continue

    df = pd.DataFrame(record.p_signal, columns=record.sig_name)
    df["record_id"] = rec_name
    df["sample_idx"] = range(len(df))
    df["time_sec"] = df["sample_idx"] / record.fs
    df["is_r_peak"] = False
    peaks = ann.sample[ann.sample < len(df)]
    df.loc[peaks, "is_r_peak"] = True

    out_csv = OUT_DIR / f"{rec_name}.csv"
    df.to_csv(out_csv, index=False)
    print(f"{rec_name}: {df.shape} -> {out_csv}")
    print(f"  sig_name = {record.sig_name}")
    print(f"  fs = {record.fs}, n_r_peaks = {len(peaks)}")
    