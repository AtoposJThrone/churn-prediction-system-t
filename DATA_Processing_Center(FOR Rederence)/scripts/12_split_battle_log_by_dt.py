import pandas as pd
import os

from config_env import env

transformed_dir = env("TD_CHURN_TRANSFORMED_DIR", "/DataSet_Transformed")
infile = os.path.join(transformed_dir, "battle_log.csv")
outdir = env("TD_CHURN_BATTLE_LOG_BY_DT_DIR", os.path.join(transformed_dir, "battle_log_by_dt"))
os.makedirs(outdir, exist_ok=True)

df = pd.read_csv(infile)
if "battle_start_time" not in df.columns:
    raise ValueError(f"Missing battle_start_time, columns={list(df.columns)}")

df["dt"] = df["battle_start_time"].astype(str).str.slice(0, 10)

for dt, sub in df.groupby("dt"):
    out = os.path.join(outdir, f"battle_log_{dt}.csv")
    sub.drop(columns=["dt"]).to_csv(out, index=False)
    print(f"write: {out} rows={len(sub)}")
