"""
transform_to_td.py
==================
将休闲闯关游戏数据改造为塔防（Tower Defense）风格的用户行为数据。

映射逻辑说明
------------
原始字段          →  塔防字段              含义
-----------          --------              ----
level_id          →  map_id               地图编号（对应塔防中的防守地图）
f_success         →  battle_result        战斗结果（1=守住/胜利，0=基地被攻破/失败）
f_duration        →  battle_duration_s    战斗持续时长（秒）
f_reststep        →  base_hp_ratio        战斗结束时基地剩余血量比例（0=被打穿）
f_help            →  used_special_tower   是否部署了特殊塔/英雄技能（0/1）
time              →  battle_start_time    战斗开始时间戳

level_meta 字段映射：
f_avg_duration       →  map_avg_duration_s      地图平均战斗时长
f_avg_passrate       →  map_clear_rate          地图平均通关率（守住率）
f_avg_win_duration   →  map_avg_win_duration_s  通关场次平均耗时
f_avg_retrytimes     →  map_avg_retry_times     平均重试次数

新增衍生字段（体现塔防特色）
------------------------------
wave_count          : 推算出的波次数（duration / 平均每波耗时）
tower_score         : 综合防守评分 = base_hp_ratio * 100 + (1-used_special_tower)*10
difficulty_tier     : 地图难度分级（easy/normal/hard/extreme），基于 map_clear_rate
session_gap_s       : 与上一场战斗的间隔时长（秒），刻画会话节奏
cumulative_battles  : 用户累计战斗场次（按时间排序）
consecutive_fail    : 连续失败次数（连败压力指标）
"""

import pandas as pd
import numpy as np
import os

from config_env import env

# ───────────────────────────── 配置 ─────────────────────────────
INPUT_DIR  = env("TD_CHURN_ORIGIN_DIR", "/DataSet_Origin")
OUTPUT_DIR = env("TD_CHURN_TRANSFORMED_DIR", "/DataSet_Transformed")

LEVEL_SEQ_FILE  = os.path.join(INPUT_DIR, "level_seq.csv")
LEVEL_META_FILE = os.path.join(INPUT_DIR, "level_meta.csv")
TRAIN_FILE      = os.path.join(INPUT_DIR, "train.csv")
DEV_FILE        = os.path.join(INPUT_DIR, "dev.csv")
TEST_FILE       = os.path.join(INPUT_DIR, "test.csv")

AVG_WAVE_DURATION_S = 15   # 假设塔防每波平均 15 秒，用于推算 wave_count
RANDOM_SEED = 42

os.makedirs(OUTPUT_DIR, exist_ok=True)
np.random.seed(RANDOM_SEED)

# ═══════════════════════════════════════════════════════════════
# 0.5. 原始数据分隔符处理
# ═══════════════════════════════════════════════════════════════
def read_any_csv(path: str) -> pd.DataFrame:
    # 自动识别分隔符（tab/逗号/分号等），并处理 UTF-8 BOM
    # sep=None has a bug about test.csv,so use the r"[\t,]"
    return pd.read_csv(path, sep=r"[\t,]", engine="python", encoding="utf-8-sig")

# ═══════════════════════════════════════════════════════════════
# 1. 读取原始数据
# ═══════════════════════════════════════════════════════════════
print(">>> 读取原始数据 ...")
seq   = read_any_csv(LEVEL_SEQ_FILE)
meta  = read_any_csv(LEVEL_META_FILE)
train = read_any_csv(TRAIN_FILE)
dev   = read_any_csv(DEV_FILE)
test  = read_any_csv(TEST_FILE)

print(f"    level_seq  : {len(seq):,} 行")
print(f"    level_meta : {len(meta):,} 行")
print(f"    train/dev/test : {len(train)}/{len(dev)}/{len(test)} 用户")

# ═══════════════════════════════════════════════════════════════
# 2. 改造 level_meta → map_meta
# ═══════════════════════════════════════════════════════════════
print("\n>>> 改造 level_meta ...")

map_meta = meta.rename(columns={
    "level_id"           : "map_id",
    "f_avg_duration"     : "map_avg_duration_s",
    "f_avg_passrate"     : "map_clear_rate",
    "f_avg_win_duration" : "map_avg_win_duration_s",
    "f_avg_retrytimes"   : "map_avg_retry_times",
})

# 难度分级：按通关率四分位划分
q = map_meta["map_clear_rate"].quantile([0.25, 0.5, 0.75]).values
map_meta["difficulty_tier"] = pd.cut(
    map_meta["map_clear_rate"],
    bins=[-np.inf, q[0], q[1], q[2], np.inf],
    labels=["extreme", "hard", "normal", "easy"]
)

# 推算地图设计波次数（map_avg_win_duration / AVG_WAVE_DURATION_S，至少1波）
map_meta["map_design_waves"] = np.maximum(
    (map_meta["map_avg_win_duration_s"] / AVG_WAVE_DURATION_S).round().astype(int), 1
)

print(f"    输出列: {list(map_meta.columns)}")
map_meta.to_csv(os.path.join(OUTPUT_DIR, "map_meta.csv"), index=False)

# ═══════════════════════════════════════════════════════════════
# 3. 改造 level_seq → battle_log
# ═══════════════════════════════════════════════════════════════
print("\n>>> 改造 level_seq ...")

battle_log = seq.rename(columns={
    "level_id"   : "map_id",
    "f_success"  : "battle_result",
    "f_duration" : "battle_duration_s",
    "f_reststep" : "base_hp_ratio",
    "f_help"     : "used_special_tower",
    "time"       : "battle_start_time",
})

battle_log["battle_start_time"] = pd.to_datetime(battle_log["battle_start_time"])

# ── 3a. 按用户时间排序后计算会话相关特征 ──
battle_log = battle_log.sort_values(["user_id", "battle_start_time"]).reset_index(drop=True)

# 与上一场战斗的时间间隔（同用户内）
battle_log["session_gap_s"] = (
    battle_log.groupby("user_id")["battle_start_time"]
    .diff()
    .dt.total_seconds()
    .fillna(0)
    .clip(lower=0)
)

# 累计战斗场次（从1开始）
battle_log["cumulative_battles"] = battle_log.groupby("user_id").cumcount() + 1

# 连续失败次数
def calc_consecutive_fail(series):
    result = []
    count = 0
    for v in series:
        if v == 0:
            count += 1
        else:
            count = 0
        result.append(count)
    return result

battle_log["consecutive_fail"] = (
    battle_log.groupby("user_id")["battle_result"]
    .transform(lambda x: calc_consecutive_fail(x.tolist()))
)

# ── 3b. 合并 map_meta 以补充地图特征 ──
battle_log = battle_log.merge(
    map_meta[["map_id", "map_design_waves", "difficulty_tier", "map_avg_duration_s"]],
    on="map_id", how="left"
)

# ── 3c. 推算本场实际达到的波次（基于时长比例）──
# 成功场：wave_count = map_design_waves
# 失败场：wave_count = round(duration / map_avg_duration_s * map_design_waves)，至少1
battle_log["wave_count"] = np.where(
    battle_log["battle_result"] == 1,
    battle_log["map_design_waves"],
    np.maximum(
        (battle_log["battle_duration_s"] / battle_log["map_avg_duration_s"].replace(0, np.nan)
         * battle_log["map_design_waves"]).round().fillna(1).astype(int),
        1
    )
)
battle_log["wave_count"] = battle_log["wave_count"].clip(upper=battle_log["map_design_waves"])

# ── 3d. 防守评分（塔防风格综合指标）──
# 守住时：base_hp_ratio 越高越好；未使用特殊塔加分（体现操作实力）
battle_log["tower_score"] = (
    battle_log["base_hp_ratio"] * 100
    + (1 - battle_log["used_special_tower"]) * 10
).round(2)

# ── 3e. 删除辅助合并列 ──
battle_log.drop(columns=["map_avg_duration_s", "map_design_waves"], inplace=True)

print(f"    输出列: {list(battle_log.columns)}")
battle_log.to_csv(os.path.join(OUTPUT_DIR, "battle_log.csv"), index=False)

# ═══════════════════════════════════════════════════════════════
# 4. 标签文件原样复制（user_id 不变，含义相同）
# ═══════════════════════════════════════════════════════════════
train.to_csv(os.path.join(OUTPUT_DIR, "train.csv"), index=False)
dev.to_csv(os.path.join(OUTPUT_DIR, "dev.csv"),   index=False)
test.to_csv(os.path.join(OUTPUT_DIR, "test.csv"),  index=False)
print("\n>>> 标签文件已原样写出（train/dev/test）")

# ═══════════════════════════════════════════════════════════════
# 5. 输出数据字典
# ═══════════════════════════════════════════════════════════════
print("\n>>> 生成数据字典 ...")

DICT_TEXT = """
# 塔防游戏用户行为数据字典

## battle_log.csv（核心行为日志）

| 字段名              | 类型    | 说明                                                     |
|---------------------|---------|----------------------------------------------------------|
| user_id             | int     | 用户 ID                                                  |
| map_id              | int     | 地图 ID（对应塔防中的防守地图）                           |
| battle_result       | int     | 战斗结果：1=守住（胜利），0=基地被攻破（失败）            |
| battle_duration_s   | float   | 战斗持续时长（秒）                                        |
| base_hp_ratio       | float   | 战斗结束时基地剩余血量比例，[0,1]，失败时为 0             |
| used_special_tower  | int     | 是否部署特殊塔/英雄技能：1=是，0=否                      |
| battle_start_time   | datetime| 战斗开始时间戳                                            |
| session_gap_s       | float   | 与上一场战斗的时间间隔（秒），同用户内，首场为 0          |
| cumulative_battles  | int     | 用户累计战斗场次（按时间正序）                            |
| consecutive_fail    | int     | 当前连续失败次数（胜利时重置为 0）                        |
| difficulty_tier     | str     | 地图难度：easy / normal / hard / extreme                 |
| wave_count          | int     | 本场实际经历的波次数                                      |
| tower_score         | float   | 综合防守评分 = base_hp_ratio*100 + (1-special_tower)*10  |

## map_meta.csv（地图元数据）

| 字段名                   | 类型    | 说明                                         |
|--------------------------|---------|----------------------------------------------|
| map_id                   | int     | 地图 ID                                      |
| map_avg_duration_s       | float   | 所有场次平均战斗时长（秒）                    |
| map_clear_rate           | float   | 地图平均通关率（守住率），[0,1]              |
| map_avg_win_duration_s   | float   | 通关场次平均耗时（秒）                        |
| map_avg_retry_times      | float   | 平均重试次数                                  |
| difficulty_tier          | str     | 难度分级：easy/normal/hard/extreme            |
| map_design_waves         | int     | 推算的地图设计波次数                          |

## train.csv / dev.csv / test.csv（标签文件）

| 字段名  | 类型 | 说明                           |
|---------|------|--------------------------------|
| user_id | int  | 用户 ID                        |
| label   | int  | 流失标签：1=流失，0=留存（test无此列）|
"""

with open(os.path.join(OUTPUT_DIR, "data_dictionary.md"), "w", encoding="utf-8") as f:
    f.write(DICT_TEXT)

print("\n✅ 数据改造完成，文件输出至:", OUTPUT_DIR)
print("   - battle_log.csv")
print("   - map_meta.csv")
print("   - train.csv / dev.csv / test.csv")
print("   - data_dictionary.md")
