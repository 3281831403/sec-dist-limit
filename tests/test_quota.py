"""分布式分片限流的行为契约测试（恰好 15 个，初始全红）。

前 10 个用例分别对应题面中的 10 个缺陷，后 5 个覆盖额度守恒、窗口边界、
扩缩容稳定性、时钟偏移排除和批量独立判定。tests 目录不可修改。
"""

from dlimit.quota import (
    admit,
    clock_skew_ok,
    local_quota,
    quota_batch,
    rebalance,
    shard_of,
    validate_policy,
    window_bounds,
)

POLICY = {
    "total": 100,
    "shards": 4,
    "window": 60,
    "max_skew": 2,
    "weights": [1, 1, 1, 1],
}


def test_defect_01_modulo_sharding_breaks_affinity_on_resize():
    """缺陷 1：取模分片在扩缩容后会让大量同键请求跳到无关分片。"""
    keys = [f"tenant-{i}" for i in range(24)]
    before = [shard_of(key, 8) for key in keys]
    after = [shard_of(key, 9) for key in keys]
    retained = sum(old == new for old, new in zip(before, after))
    assert retained >= 18


def test_defect_02_case_insensitive_sharding_merges_distinct_keys():
    """缺陷 2：大小写不敏感会把不同键合并，必须保持键的精确语义。"""
    different = 0
    for index in range(8):
        upper = shard_of(f"User-{index}", 64)
        lower = shard_of(f"user-{index}", 64)
        if upper != lower:
            different += 1
    assert different >= 6


def test_defect_03_closed_window_double_counts_boundary():
    """缺陷 3：窗口是左开右闭区间，start 时刻的请求不能被当前窗口再算一次。"""
    start, end = window_bounds(100, 10)
    previous_start, previous_end = window_bounds(90, 10)

    assert (start, end) == (90, 100)
    assert (previous_start, previous_end) == (80, 90)
    assert sum(start < timestamp <= end for timestamp in (90, 95, 100)) == 2
    assert sum(previous_start < timestamp <= previous_end for timestamp in (90,)) == 1

    policy = {**POLICY, "total": 10, "shards": 1, "window": 10, "max_skew": 0}

    at_boundary = {
        "balance": 10,
        "events": [{"at": 90, "key": "old", "cost": 4}],
    }
    boundary = admit(at_boundary, "alpha", 7, 100, policy)
    assert boundary["allowed"] is True
    assert boundary["remaining"] == 3

    inside_window = {
        "balance": 10,
        "events": [{"at": 95, "key": "old", "cost": 4}],
    }
    inside = admit(inside_window, "alpha", 7, 100, policy)
    assert inside["allowed"] is False
    assert inside["remaining"] == 10
    assert len(inside_window["events"]) == 1


def test_defect_04_local_quota_ceiling_exceeds_total():
    """缺陷 4：单机额度向上取整会让所有分片加起来超过全局总额。"""
    quota = local_quota(100, 8, 0)
    assert quota == 12
    assert quota * 8 <= 100
    assert local_quota(10, 3, 0) == 3


def test_defect_05_weighted_rebalance_starves_hot_shard():
    """缺陷 5：额度折算必须尊重权重，热点分片不能只拿到均分额度。"""
    weighted = rebalance(100, 2, [3, 1])
    assert weighted == {0: 75, 1: 25}
    assert sum(weighted.values()) == 100

    zero_weight = rebalance(90, 3, [2, 1, 0])
    assert zero_weight == {0: 60, 1: 30, 2: 0}
    assert sum(zero_weight.values()) == 90


def test_defect_06_rebalance_remainder_is_deterministic():
    """缺陷 6：余数必须确定性分配，并且要给小数部分最大的分片。"""
    assert rebalance(7, 3, [1, 2, 3]) == {0: 1, 1: 2, 2: 4}

    expected = {0: 4, 1: 3, 2: 3}
    for _ in range(32):
        assert rebalance(10, 3, [1, 1, 1]) == expected


def test_defect_07_insufficient_balance_is_not_partially_deducted():
    """缺陷 7：余额不足时整笔拒绝，不能扣一半或写负余额。"""
    policy = {**POLICY, "total": 10, "shards": 1}
    state = {"balance": 3, "events": []}

    rejected = admit(state, "alpha", 5, 100, policy)
    assert rejected["allowed"] is False
    assert rejected["remaining"] == 3
    assert state["balance"] == 3
    assert state["events"] == []

    accepted = admit(state, "alpha", 3, 100, policy)
    assert accepted["allowed"] is True
    assert accepted["remaining"] == 0
    assert state["balance"] == 0
    assert len(state["events"]) == 1
    assert state["events"][0]["cost"] == 3


def test_defect_08_clock_skew_over_limit_is_not_counted():
    """缺陷 8：时钟偏移超过上限的分片必须被排除，不能放大全局额度。"""
    assert clock_skew_ok(110, 100, 10) is True
    assert clock_skew_ok(90, 100, 10) is True
    assert clock_skew_ok(111, 100, 10) is False
    assert clock_skew_ok(89, 100, 10) is False
    assert clock_skew_ok(100, 100, 0) is True
    assert clock_skew_ok(101, 100, 0) is False
    assert clock_skew_ok(101, 100, -1) is False


def test_defect_09_validate_policy_rejects_zero_window_without_dividing():
    """缺陷 9：窗口为零必须报问题，不能先做除法后变成全部放行。"""
    zero_window = {"total": 100, "shards": 4, "window": 0, "max_skew": 0}
    assert validate_policy(zero_window) == ["window_zero"]

    missing_shards = {"total": 100, "shards": 0, "window": 60, "max_skew": 2}
    assert validate_policy(missing_shards) == ["shards_missing"]

    too_large_skew = {"total": 100, "shards": 4, "window": 60, "max_skew": 61}
    assert validate_policy(too_large_skew) == ["max_skew_too_large"]
    assert validate_policy(POLICY) == []


def test_defect_10_batch_reuses_first_key_balance():
    """缺陷 10：批量判定必须逐键使用自己的余额，后一个键不能花前一个键的额度。"""
    requests = [
        {"key": "alpha", "cost": 4, "balance": 5},
        {"key": "beta", "cost": 4, "balance": 5},
    ]
    assert quota_batch(requests, POLICY, 100) == {
        "alpha": {"allowed": True, "remaining": 1},
        "beta": {"allowed": True, "remaining": 1},
    }


def test_extra_01_local_quota_sum_never_exceeds_total():
    """额外：任意总额、分片数和预留量下，单机额度乘分片数都不超过总额。"""
    for total in (0, 1, 5, 10, 100, 101):
        for shards in (1, 3, 8):
            for skew in (0, 1, 7):
                quota = local_quota(total, shards, skew)
                assert quota >= 0
                assert quota == max(0, total - skew) // shards
                assert quota * shards <= total


def test_extra_02_boundary_instant_counted_once():
    """额外：相邻窗口共享端点，端点上的请求只能被计算一次。"""
    current = window_bounds(100, 10)
    following = window_bounds(110, 10)
    assert current == (90, 100)
    assert following == (100, 110)

    events = [100]
    current_count = sum(current[0] < at <= current[1] for at in events)
    following_count = sum(following[0] < at <= following[1] for at in events)
    assert current_count == 1
    assert following_count == 0
    assert current_count + following_count == 1


def test_extra_03_resize_moves_keys_only_to_new_shard():
    """额外：增加一个分片时，已有键只能留在原分片或迁往新分片。"""
    keys = [f"session-{i}" for i in range(64)]
    before = [shard_of(key, 8) for key in keys]
    after = [shard_of(key, 9) for key in keys]

    assert all(0 <= index < 8 for index in before)
    assert all(0 <= index < 9 for index in after)

    moved = [(old, new) for old, new in zip(before, after) if old != new]
    assert moved
    assert all(new == 8 for _, new in moved)


def test_extra_04_skewed_shards_excluded_from_aggregation():
    """额外：偏移超限的分片不进入统计，只保留偏移在范围内的分片。"""
    clocks = [
        (0, 100),
        (1, 106),
        (2, 95),
        (3, 89),
    ]
    eligible = [
        shard
        for shard, remote_now in clocks
        if clock_skew_ok(remote_now, 100, 5)
    ]
    assert eligible == [0, 2]


def test_extra_05_independent_batches_match_individual_admission():
    """额外：两批独立判定互不影响，且与逐条单独判定完全一致。"""
    requests = [
        {"key": "alpha", "cost": 4, "balance": 5},
        {"key": "beta", "cost": 6, "balance": 5},
        {"key": "gamma", "cost": 2, "balance": 2},
    ]
    original = [dict(request) for request in requests]

    batch = quota_batch(requests, POLICY, 100)
    expected = {
        "alpha": {"allowed": True, "remaining": 1},
        "beta": {"allowed": False, "remaining": 5},
        "gamma": {"allowed": True, "remaining": 0},
    }
    assert batch == expected
    assert quota_batch(requests, POLICY, 100) == batch

    individual = {}
    for request in requests:
        state = {"balance": request["balance"], "events": []}
        decision = admit(state, request["key"], request["cost"], 100, POLICY)
        individual[request["key"]] = {
            "allowed": decision["allowed"],
            "remaining": decision["remaining"],
        }

    assert individual == batch
    assert requests == original