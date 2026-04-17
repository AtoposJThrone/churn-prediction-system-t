"""
step3_eda.py
============
阶段一：特征探索性分析（EDA）
在正式训练之前运行，目的是：
  1. 掌握数据基本情况（样本量、标签分布、类别不平衡程度）
  2. 发现需要丢弃的特征（常量列、极高缺失率列）
  3. 发现高度相关的冗余特征对（相关系数 > 0.95 的保留一个）
  4. 理解流失用户 vs 留存用户在关键特征上的分布差异
  5. 生成所有图表和统计报告，供论文"特征工程"章节使用

运行方式（在 master 上）：
  python3 step3_eda.py

输出：
  eda_output/
    ├── label_dist.png          标签分布柱状图
    ├── feature_nullrate.csv    各特征缺失率
    ├── corr_matrix.png         特征相关性热力图
    ├── drop_features.txt       建议丢弃的特征列表
    ├── kept_features.txt       建议保留的特征列表（直接供训练脚本使用）
    ├── dist_churn_vs_stay/     流失 vs 留存分布图（每个关键特征一张）
    └── eda_summary.txt         文字版统计摘要
"""

import os
import warnings
import numpy  as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")          # 无桌面环境用非交互后端
import matplotlib.pyplot as plt
import seaborn as sns
from scipy import stats

from config_env import env

plt.rcParams['font.sans-serif'] = ['Noto Sans CJK JP']
plt.rcParams['axes.unicode_minus'] = False

warnings.filterwarnings("ignore")

# ─────────────────────── 配置 ──────────────────────────────────
PROJECT_DIR  = env("TD_CHURN_PROJECT_DIR", "/home/hadoop/td_churn_project")
FEATURE_DIR  = env("TD_CHURN_ETL_OUTPUT_DIR", f"{PROJECT_DIR}/etl_output_v2")
OUTPUT_DIR   = env("TD_CHURN_EDA_OUTPUT_DIR", f"{PROJECT_DIR}/eda_output")
DIST_DIR     = os.path.join(OUTPUT_DIR, "dist_churn_vs_stay")

NULL_RATE_THRESHOLD  = 0.50   # 缺失率超过此值的特征直接丢弃
VARIANCE_THRESHOLD   = 1e-6   # 方差低于此值视为常量列，丢弃
CORR_THRESHOLD       = 0.95   # 相关系数超过此值的特征对，丢弃其中一个

os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(DIST_DIR,   exist_ok=True)

# ─────────────────────── 工具函数 ──────────────────────────────
def save_fig(fig, name):
    path = os.path.join(OUTPUT_DIR, name)
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  图表已保存: {path}")

# ══════════════════════════════════════════════════════════════
# 1. 加载数据
# ══════════════════════════════════════════════════════════════
print("=" * 60)
print("[1] 加载特征表")
print("=" * 60)

train = pd.read_parquet(f"{FEATURE_DIR}/features_train.parquet")
dev   = pd.read_parquet(f"{FEATURE_DIR}/features_dev.parquet")

# 合并 train+dev 做 EDA（不涉及测试集，避免数据泄露）
df = pd.concat([train, dev], ignore_index=True)

NON_FEAT = {"user_id", "label", "split", "first_stuck_map_id"}
feat_cols = [c for c in df.columns if c not in NON_FEAT]

print(f"  总样本数      : {len(df):,}")
print(f"  特征列数      : {len(feat_cols)}")
print(f"  流失用户(1)   : {df['label'].sum():,}  ({df['label'].mean():.1%})")
print(f"  留存用户(0)   : {(df['label']==0).sum():,}  ({1-df['label'].mean():.1%})")

# 类别不平衡比值（正负样本比）
pos = df['label'].sum()
neg = (df['label']==0).sum()
imbalance_ratio = neg / pos
print(f"\n  正负样本比    : 1 : {imbalance_ratio:.2f}")
if imbalance_ratio > 3:
    print("  ⚠️  类别不平衡较严重，训练时需设置 scale_pos_weight 或使用 class_weight")

# ══════════════════════════════════════════════════════════════
# 2. 标签分布图
# ══════════════════════════════════════════════════════════════
print("\n[2] 标签分布图")
fig, ax = plt.subplots(figsize=(5, 4))
counts = df['label'].value_counts().sort_index()
bars = ax.bar(['留存(0)', '流失(1)'], counts.values,
               color=['#4C9BE8', '#E8654C'], width=0.5, edgecolor='white')
for bar, v in zip(bars, counts.values):
    ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 20,
            f'{v:,}\n({v/len(df):.1%})', ha='center', va='bottom', fontsize=10)
ax.set_title('用户标签分布（train + dev）', fontsize=13)
ax.set_ylabel('用户数')
ax.spines[['top','right']].set_visible(False)
save_fig(fig, "label_dist.png")

# ══════════════════════════════════════════════════════════════
# 3. 缺失率统计
# ══════════════════════════════════════════════════════════════
print("\n[3] 特征缺失率统计")
null_rates = df[feat_cols].isnull().mean().sort_values(ascending=False)
null_df = null_rates.reset_index()
null_df.columns = ["feature", "null_rate"]
null_df.to_csv(os.path.join(OUTPUT_DIR, "feature_nullrate.csv"), index=False)

high_null = null_df[null_df["null_rate"] > NULL_RATE_THRESHOLD]
print(f"  缺失率 > {NULL_RATE_THRESHOLD:.0%} 的特征（建议丢弃）: {len(high_null)} 个")
if len(high_null):
    print(high_null.to_string(index=False))

# 可视化缺失率（只展示有缺失的）
has_null = null_df[null_df["null_rate"] > 0]
if len(has_null):
    fig, ax = plt.subplots(figsize=(10, max(4, len(has_null)*0.3)))
    has_null_sorted = has_null.sort_values("null_rate")
    colors = ['#E8654C' if r > NULL_RATE_THRESHOLD else '#4C9BE8'
              for r in has_null_sorted["null_rate"]]
    ax.barh(has_null_sorted["feature"], has_null_sorted["null_rate"]*100, color=colors)
    ax.axvline(NULL_RATE_THRESHOLD*100, color='red', linestyle='--',
               label=f'阈值 {NULL_RATE_THRESHOLD:.0%}')
    ax.set_xlabel('缺失率 (%)')
    ax.set_title('特征缺失率（红色=超阈值）')
    ax.legend()
    save_fig(fig, "feature_nullrate.png")

# ══════════════════════════════════════════════════════════════
# 4. 常量列检测（方差接近0）
# ══════════════════════════════════════════════════════════════
print("\n[4] 常量/低方差特征检测")
variances    = df[feat_cols].var()
low_var_cols = variances[variances < VARIANCE_THRESHOLD].index.tolist()
print(f"  低方差特征（建议丢弃）: {len(low_var_cols)} 个")
if low_var_cols:
    print(f"  {low_var_cols}")

# ══════════════════════════════════════════════════════════════
# 5. 高相关特征对检测
# ══════════════════════════════════════════════════════════════
print("\n[5] 高相关特征对检测（阈值 > {})".format(CORR_THRESHOLD))

# 用完整数据填充后计算相关矩阵
df_filled = df[feat_cols].fillna(df[feat_cols].median())
corr_matrix = df_filled.corr(method='pearson')

# 找出相关系数超阈值的特征对
corr_pairs = []
cols = corr_matrix.columns
for i in range(len(cols)):
    for j in range(i+1, len(cols)):
        c = abs(corr_matrix.iloc[i, j])
        if c > CORR_THRESHOLD:
            corr_pairs.append((cols[i], cols[j], round(c, 4)))

corr_pairs_df = pd.DataFrame(corr_pairs, columns=["feat_a", "feat_b", "corr"])
corr_pairs_df = corr_pairs_df.sort_values("corr", ascending=False)
print(f"  相关系数 > {CORR_THRESHOLD} 的特征对: {len(corr_pairs_df)} 对")
if len(corr_pairs_df):
    print(corr_pairs_df.to_string(index=False))

# 贪心删除策略：在每对中删除"被更多特征高度相关的那个"
to_drop_corr = set()
for _, row in corr_pairs_df.iterrows():
    a, b = row["feat_a"], row["feat_b"]
    if a not in to_drop_corr and b not in to_drop_corr:
        # 保留与其他特征相关性更低的那个（删除"更冗余"的）
        a_max_corr = corr_matrix[a].drop(a).abs().max()
        b_max_corr = corr_matrix[b].drop(b).abs().max()
        to_drop_corr.add(a if a_max_corr > b_max_corr else b)

print(f"\n  贪心策略建议删除的冗余特征: {len(to_drop_corr)} 个")
print(f"  {sorted(to_drop_corr)}")

# 相关性热力图（只画 top 30 个特征，否则太密）
top_feats = df[feat_cols].fillna(0).corrwith(df['label']).abs() \
                          .sort_values(ascending=False).head(30).index.tolist()
sub_corr = df_filled[top_feats].corr()
fig, ax = plt.subplots(figsize=(14, 12))
mask = np.triu(np.ones_like(sub_corr, dtype=bool))
sns.heatmap(sub_corr, mask=mask, annot=False, cmap="RdYlGn_r",
            vmin=-1, vmax=1, ax=ax, linewidths=0.5)
ax.set_title("Top-30 特征相关性矩阵（下三角）", fontsize=13)
plt.xticks(rotation=45, ha='right', fontsize=7)
plt.yticks(fontsize=7)
save_fig(fig, "corr_matrix.png")

# ══════════════════════════════════════════════════════════════
# 6. 特征与标签的单变量相关性排序
# ══════════════════════════════════════════════════════════════
print("\n[6] 特征与流失标签的单变量相关性（Point-Biserial）")
label_corr = df[feat_cols].fillna(df[feat_cols].median()) \
                           .corrwith(df['label']) \
                           .abs().sort_values(ascending=False)
label_corr_df = label_corr.reset_index()
label_corr_df.columns = ["feature", "abs_corr_with_label"]
print(label_corr_df.head(20).to_string(index=False))
label_corr_df.to_csv(os.path.join(OUTPUT_DIR, "feature_label_corr.csv"), index=False)

# 可视化 Top20
fig, ax = plt.subplots(figsize=(9, 6))
top20 = label_corr_df.head(20).sort_values("abs_corr_with_label")
ax.barh(top20["feature"], top20["abs_corr_with_label"], color="#4C9BE8")
ax.set_xlabel("与流失标签的相关系数（绝对值）")
ax.set_title("Top-20 特征与流失标签相关性")
ax.spines[['top','right']].set_visible(False)
save_fig(fig, "feature_label_corr_top20.png")

# ══════════════════════════════════════════════════════════════
# 7. 关键特征的流失 vs 留存分布对比
# ══════════════════════════════════════════════════════════════
print("\n[7] 关键特征分布对比图（流失 vs 留存）")

# 取与标签相关性最高的 12 个特征
key_feats = label_corr_df.head(12)["feature"].tolist()

for feat in key_feats:
    col = df[feat].fillna(df[feat].median())
    # 截断极端值（99.5 百分位）防止长尾影响图形
    cap = col.quantile(0.995)
    col = col.clip(upper=cap)

    churn_vals = col[df['label'] == 1]
    stay_vals  = col[df['label'] == 0]

    fig, ax = plt.subplots(figsize=(7, 4))
    ax.hist(stay_vals,  bins=40, alpha=0.6, color='#4C9BE8',
            density=True, label=f'留存(n={len(stay_vals):,})')
    ax.hist(churn_vals, bins=40, alpha=0.6, color='#E8654C',
            density=True, label=f'流失(n={len(churn_vals):,})')

    # KS 检验：两组分布是否显著不同
    ks_stat, ks_p = stats.ks_2samp(stay_vals, churn_vals)
    ax.set_title(f'{feat}\nKS={ks_stat:.3f}, p={ks_p:.2e}', fontsize=11)
    ax.set_ylabel('密度')
    ax.legend()
    ax.spines[['top','right']].set_visible(False)

    save_path = os.path.join(DIST_DIR, f"dist_{feat}.png")
    fig.savefig(save_path, dpi=120, bbox_inches="tight")
    plt.close(fig)

print(f"  分布图已保存至: {DIST_DIR}/")

# ══════════════════════════════════════════════════════════════
# 8. 汇总建议：最终保留的特征列表
# ══════════════════════════════════════════════════════════════
print("\n[8] 汇总特征筛选结论")

drop_high_null = set(high_null["feature"].tolist())
drop_low_var   = set(low_var_cols)
drop_all       = drop_high_null | drop_low_var | to_drop_corr

kept_features = [c for c in feat_cols if c not in drop_all]
print(f"  原始特征数   : {len(feat_cols)}")
print(f"  高缺失率删除 : {len(drop_high_null)}")
print(f"  低方差删除   : {len(drop_low_var)}")
print(f"  高相关冗余   : {len(to_drop_corr)}")
print(f"  最终保留特征 : {len(kept_features)}")

# 写出特征列表文件（供 step3_train.py 直接读取）
with open(os.path.join(OUTPUT_DIR, "drop_features.txt"), "w") as f:
    for c in sorted(drop_all):
        f.write(c + "\n")

with open(os.path.join(OUTPUT_DIR, "kept_features.txt"), "w") as f:
    for c in kept_features:
        f.write(c + "\n")

# ══════════════════════════════════════════════════════════════
# 9. 文字摘要（写入 txt，方便粘贴进论文）
# ══════════════════════════════════════════════════════════════
summary_lines = [
    "=" * 60,
    "EDA 统计摘要",
    "=" * 60,
    f"总样本数          : {len(df):,}",
    f"特征列数（原始）   : {len(feat_cols)}",
    f"流失用户数         : {int(pos):,} ({pos/len(df):.1%})",
    f"留存用户数         : {int(neg):,} ({neg/len(df):.1%})",
    f"正负样本比         : 1 : {imbalance_ratio:.2f}",
    "",
    f"--- 特征筛选 ---",
    f"缺失率>{NULL_RATE_THRESHOLD:.0%} 删除  : {len(drop_high_null)} 个",
    f"低方差删除         : {len(drop_low_var)} 个",
    f"高相关冗余删除     : {len(to_drop_corr)} 个",
    f"最终保留特征       : {len(kept_features)} 个",
    "",
    f"--- Top10 与标签相关特征 ---",
]
for _, row in label_corr_df.head(10).iterrows():
    summary_lines.append(f"  {row['feature']:<45} {row['abs_corr_with_label']:.4f}")

summary_text = "\n".join(summary_lines)
print("\n" + summary_text)

with open(os.path.join(OUTPUT_DIR, "eda_summary.txt"), "w", encoding="utf-8") as f:
    f.write(summary_text)

print(f"\n✅ EDA 完成，所有输出保存至: {OUTPUT_DIR}/")
