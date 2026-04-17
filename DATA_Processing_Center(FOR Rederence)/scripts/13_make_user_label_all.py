import pandas as pd
import os

from config_env import env

transformed_dir = env("TD_CHURN_TRANSFORMED_DIR", "/DataSet_Transformed")
train = pd.read_csv(os.path.join(transformed_dir, "train.csv"))
dev   = pd.read_csv(os.path.join(transformed_dir, "dev.csv"))
test  = pd.read_csv(os.path.join(transformed_dir, "test.csv"))

# 列检查
for name, df in [("train", train), ("dev", dev)]:
    if not set(["user_id","label"]).issubset(df.columns):
        raise ValueError(f"{name} missing columns, got {list(df.columns)}")
if "user_id" not in test.columns:
    raise ValueError(f"test missing user_id, got {list(test.columns)}")

train["split"] = "train"
dev["split"] = "dev"
test["label"] = pd.NA
test["split"] = "test"

all_df = pd.concat([train, dev, test], ignore_index=True)[["user_id","label","split"]]
output_path = os.path.join(transformed_dir, "user_label_all.csv")
all_df.to_csv(output_path, index=False)
print("write:", output_path, "rows=", len(all_df))
print(all_df["split"].value_counts(dropna=False))
