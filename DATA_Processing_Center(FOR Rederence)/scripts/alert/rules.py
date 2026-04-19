"""
alert.rules
============
Hard-coded business rules for churn risk early warning.

Each rule is a dict with:
  code, name, description, condition (lambda on user_row dict), force_high.
"""

RULES = [
    {
        "code": "R001",
        "name": "First-day tutorial incomplete",
        "description": "用户注册首日未通过第1关（新手引导），对游戏核心循环理解不足，流失风险极高",
        "condition": lambda r: r.get("d1_completed_map1", 1) == 0
        and r.get("d1_battles", 0) >= 1,
        "force_high": True,
    },
    {
        "code": "R002",
        "name": "First-day zero battles",
        "description": "用户注册首日未进行任何战斗，可能注册后立即流失",
        "condition": lambda r: r.get("d1_battles", 0) == 0,
        "force_high": True,
    },
    {
        "code": "R003",
        "name": "Severe stuck on multiple maps",
        # Semantic fix: more precise description
        "description": "多个地图卡关且整体胜率低，关卡推进严重受阻",
        "condition": lambda r: r.get("stuck_level_count", 0) >= 3
        and r.get("overall_win_rate", 1) < 0.3,
        "force_high": False,
    },
    {
        "code": "R004",
        "name": "Recent win-rate plunge",
        "description": "用户最近10场胜率显著低于整体胜率（下降超过30%），游戏体验恶化",
        "condition": lambda r: (r.get("overall_win_rate", 0) - r.get("recent10_win_rate", 0)) > 0.30
        and r.get("total_battles", 0) >= 20,
        "force_high": False,
    },
    {
        "code": "R005",
        "name": "Session gap abnormally long",
        "description": "用户最近平均会话间隔超过12小时，活跃度已显著下降",
        "condition": lambda r: r.get("avg_session_gap_s", 0) > 43200,
        "force_high": False,
    },
    {
        "code": "R006",
        "name": "High item dependency but still losing",
        "description": "用户高频使用特殊塔但胜率仍低，说明技能与关卡难度严重不匹配",
        "condition": lambda r: r.get("special_tower_use_rate", 0) > 0.7
        and r.get("overall_win_rate", 1) < 0.4,
        "force_high": False,
    },
    {
        "code": "R007",
        "name": "Low activity in first 3 days",
        "description": "注册后前3天仅活跃1天，早期留存信号极差",
        "condition": lambda r: r.get("d3_active_days", 3) <= 1
        and r.get("total_battles", 0) > 0,
        "force_high": False,
    },
    {
        "code": "R008",
        "name": "Level progress stalled",
        "description": "用户已有一定活跃天数但关卡推进极慢（每天不足半关），说明玩家正反馈不足，随时可能润",
        "condition": lambda r: r.get("progress_velocity", 1.0) < 0.5
        and r.get("active_days", 0) >= 3,
        "force_high": False,
    },
    {
        "code": "R009",
        "name": "First-day stuck on first level",
        "description": "用户首日只接触了一个关卡却反复尝试，说明新手引导设计存在缺陷或玩家能力较差",
        "condition": lambda r: r.get("d1_unique_maps", 2) == 1
        and r.get("d1_battles", 0) >= 3,
        "force_high": False,
    },
]


def apply_rules(user_row: dict) -> tuple:
    """
    Apply all rules to a single user row.

    Returns (force_high, triggered_codes, descriptions).
    """
    force_high = False
    triggered = []
    descriptions = []

    for rule in RULES:
        try:
            if rule["condition"](user_row):
                triggered.append(rule["code"])
                descriptions.append(rule["description"])
                if rule["force_high"]:
                    force_high = True
        except Exception as _e:
            print(f"  [WARN] Rule {rule['code']} error (possible missing feature): {_e}")

    return force_high, triggered, descriptions


# Feature -> human-readable churn reason mapping
FEATURE_REASON_MAP = {
    "d1_completed_map1":      "首日未通过新手引导关卡",
    "d1_battles":             "首日战斗场次极少",
    "overall_win_rate":       "整体胜率偏低",
    "recent10_win_rate":      "近期胜率下滑明显",
    "max_consecutive_fail":   "曾出现长时间连败",
    "stuck_level_count":      "在多个关卡反复卡关",
    "avg_session_gap_s":      "游戏会话间隔过长",
    "active_days":            "整体活跃天数少",
    "d3_active_days":         "前3天活跃度低",
    "hard_map_special_rate":  "高难度关卡过度依赖道具",
    "fail_special_rate":      "失败时仍大量消耗道具（体验差）",
    "battles_per_active_day": "日均战斗场次少，粘性不足",
    "max_map_reached":        "关卡进度停滞",
    "avg_tower_score":        "防守表现持续较差",
    "narrow_win_rate":        "残血局多，长期处于高压力局面（小失即导致流失）",
    "first_attempt_win_rate": "首次尝试胜率低，关卡难度进阶时沉没感强",
    "progress_velocity":      "关卡推进速度极慢，玩家处于游戏瓶颈期",
    "d1_unique_maps":         "首日仅探索了一个关卡且多次尝试失败，入门关设计缺陷明显",
}


def get_top_reasons(user_row: dict, rule_descs: list, n=3) -> list:
    """
    Combine rule-triggered descriptions with heuristic feature-based reasons.
    Returns up to n reason strings.
    """
    reasons = list(rule_descs)

    if len(reasons) < n:
        bad_low = {
            "overall_win_rate", "recent10_win_rate", "active_days",
            "d1_battles", "d3_active_days", "battles_per_active_day",
            "max_map_reached", "avg_tower_score", "d1_completed_map1",
            "first_attempt_win_rate", "progress_velocity", "d1_unique_maps",
        }
        bad_high = {
            "avg_session_gap_s", "max_consecutive_fail", "stuck_level_count",
            "fail_special_rate", "narrow_win_rate",
        }

        for feat, reason_text in FEATURE_REASON_MAP.items():
            if feat in user_row and reason_text not in reasons:
                val = user_row.get(feat, 0) or 0
                if (feat in bad_low and val < 0.3) or (feat in bad_high and val > 0.7):
                    reasons.append(reason_text)
            if len(reasons) >= n:
                break

    return reasons[:n]
