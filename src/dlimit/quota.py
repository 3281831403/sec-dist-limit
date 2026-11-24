"""分布式分片限流：一致性哈希、滑动窗口、额度折算与批量判定。

本模块只包含 8 个待实现函数，不读取真实时钟、不使用随机数、不联网。
所有测试都显式传入时间与状态，因此同一输入必须得到同一结果。

时间与窗口
----------
now 是整数时间戳。window_bounds 返回 (start, end)，表示左开右闭区间
(start, end]：start 时刻的事件属于上一个窗口，end 时刻的事件属于当前窗口。

额度与状态
----------
policy 描述全局额度与分片方式，典型字段为 total、shards、window、max_skew、
weights。state 描述单个本地分片的可变状态，典型字段为 balance 与 events。
balance 是当前可用额度，events 是成功扣减的事件审计列表。

安全目标
--------
同一个键扩缩容后不能漂移到无关分片；大小写不同的键不能被合并；窗口边界
不能重复计数；各分片额度之和不能超过全局总额；热点分片不能因为均分而饿死；
余数分配必须可复现；余额不足时必须整笔拒绝；超限时钟偏移不能进入统计；
零窗口必须在做除法前被发现；批量判定不能让不同键共用本地余额。
"""

from __future__ import annotations

from typing import Any, Mapping, Sequence


def shard_of(key: str, shards: int) -> int:
    """按一致性哈希把键映射到分片，返回 0 到 shards - 1 的下标。

    契约
    ----
    - 同一个键在分片数不变时必须始终返回同一个下标。
    - 必须保持键的精确语义，不能折叠大小写、不能去空白、不能做 Unicode
      归一化；User-A 与 user-a 是两个不同的键。
    - 使用一致性哈希而不是简单取模。只增加一个新分片时，已有键要么留在原
      分片，要么迁往这个新分片；不能跳到其他旧分片。缩容时同理，只允许
      受影响分片上的键迁移。
    - shards 必须是正整数，否则抛 ValueError。
    """
    raise NotImplementedError


def window_bounds(now: int, window: int) -> tuple:
    """返回以 now 为右端点的滑动窗口边界 (start, end)。

    契约
    ----
    - 窗口语义是左开右闭区间 (start, end]，即 start 时刻的请求不属于本窗口，
      end 时刻的请求只属于本窗口。
    - 返回 (now - window, now)。window 必须是正整数，否则抛 ValueError。
    - 相邻窗口共享一个端点，但该端点上的请求只能落入右端窗口，不能同时落入
      两个窗口。
    """
    raise NotImplementedError


def local_quota(total: int, shards: int, skew: int) -> int:
    """把全局总额度折算成单机均分额度，并保证总和不超过 total。

    契约
    ----
    - skew 是全局时钟偏移预留量，先从 total 中整体扣除，再把剩余额度均分到
      shards 台机器，向下取整；不能先除后扣，也不能向上取整。
    - 返回值必须是非负整数，且 local_quota(...) * shards <= total。
    - 剩余额度不足或为负时返回 0。shards 必须是正整数，否则抛 ValueError。
    - 均权只是保守回退。存在非均权 weights 时，加权分配由 rebalance 负责；
      不能把均分结果直接用于热点分片。
    """
    raise NotImplementedError


def admit(state: dict, key: str, cost: int, now: int, policy: dict) -> dict:
    """判定单条请求并扣减本地余额，余额或窗口额度不足时整笔拒绝。

    参数
    ----
    state : dict
        至少含 balance；events 可为空列表。events 中每条记录含 at、key、cost。
        成功扣减时向 events 追加 {"at": now, "key": key, "cost": cost}。
    key : str
        请求键，只用于审计与结果回显，不改变 state 的余额归属。
    cost : int
        本次请求消耗的额度，必须是非负整数。
    now : int
        请求时刻，由调用方显式传入。
    policy : dict
        限流策略，提供 total、shards、window；可选 skew 是全局偏移预留量，
        缺省为 0。本函数不读取真实时钟，也不修改 policy。

    返回
    ----
    dict
        {"allowed": bool, "remaining": int, "key": str, "at": int}。
        成功时 remaining 是扣减后的余额；失败时 remaining 与调用前一致。

    契约
    ----
    - 本地额度按 local_quota(total, shards, skew) 计算；当前窗口用量只累计
      window_bounds(now, window) 左开右闭区间内的成功事件。余额不足，或
      窗口用量加本次 cost 超过本地额度，都必须整笔拒绝。
    - 允许时一次扣掉整笔 cost，余额不得为负；events 追加一条完整记录。
    - 拒绝时不得扣一半、不得先扣后补、不得写负余额，balance 与 events
      全部保持不变。
    - 不得使用模块级全局余额，否则多个独立 state 会互相耗尽额度。
    """
    raise NotImplementedError


def rebalance(total: int, shards: int, weights: Sequence[int]) -> dict:
    """按权重把总额度重新分配到各分片，余数确定性分配。

    参数
    ----
    total : int
        待分配总额度，必须是非负整数。
    shards : int
        分片数，必须是正整数。
    weights : Sequence[int]
        每个分片的非负权重，长度必须等于 shards。

    返回
    ----
    dict
        {分片下标: 额度}，键为 0 到 shards - 1，所有额度之和等于 total。

    契约
    ----
    - 按权重比例分配，不能均分；热点分片的额度必须与权重成比例，不能被饿死。
    - 先向下取整，再把剩余整数单位按小数部分从大到小分配；小数部分相同时按
      分片下标从小到大分配，保证同一输入永远得到同一结果。
    - 不得使用随机数、不得依赖字典遍历顺序、不得把余数集中给最后一个分片。
    - weights 长度必须等于 shards；权重全为零时退化为均权。任何权重为负、
      长度不匹配或 shards 非正时抛 ValueError。
    """
    raise NotImplementedError


def clock_skew_ok(remote_now: int, local_now: int, max_skew: int) -> bool:
    """判断远端时钟偏移是否在允许范围内，超限分片必须被排除。

    契约
    ----
    - 绝对偏移 abs(remote_now - local_now) 小于等于 max_skew 时返回 True，
      严格大于时返回 False。
    - 边界值必须接受：偏移恰好等于 max_skew 仍然有效。
    - max_skew 为负时按 0 处理，不得把负上限解释成无限宽松。
    - 本函数只回答是否可参与统计；超限分片不得把额度汇总进全局。
    """
    raise NotImplementedError


def validate_policy(policy: Mapping[str, Any]) -> list[str]:
    """校验限流策略，返回按固定顺序排列的问题码列表。

    契约
    ----
    按下述顺序检查，只返回发现的问题；全部合法时返回空列表：

    1. shards 缺失、不是正整数或小于 1：返回 shards_missing，表示没有按分片
       折算。
    2. window 缺失、不是正整数或小于 1：返回 window_zero。这里必须只报告
       问题，绝不能先做除法，否则窗口为零时异常会变成全部放行。
    3. max_skew 大于 window：返回 max_skew_too_large，表示偏移上限过宽。

    同一策略可能同时有多个问题，结果顺序必须是 shards_missing、
    window_zero、max_skew_too_large。
    """
    raise NotImplementedError


def quota_batch(requests: Sequence[Mapping[str, Any]], policy: dict, now: int) -> dict:
    """批量判定多条请求，每条请求使用自己的本地余额独立判定。

    参数
    ----
    requests : Sequence[Mapping[str, Any]]
        每条请求含 key、cost、balance。balance 是该键自己的可用额度。
    policy : dict
        限流策略，原样传给单条判定逻辑。
    now : int
        本批请求共用的时刻。

    返回
    ----
    dict
        {key: {"allowed": bool, "remaining": int}}，顺序与 requests 一致；
        同一批内 key 不重复。

    契约
    ----
    - 每条请求从自己的 balance 开始，不能复用上一条请求的余额、不能共用一个
      局部变量，也不能让后一个键命中前一个键的额度。
    - 每条请求的判定与单独调用 admit 完全一致；余额不足的请求不扣减自己的
      balance。
    - 两批独立调用互不影响，不得留下模块级或闭包级的跨批状态；不修改
      requests 与 policy。
    """
    raise NotImplementedError