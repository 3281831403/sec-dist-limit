网关扩到八台之后，同一用户的总请求量涨了八倍，单机限流完全失效：分片用取模而不是一致性哈希，扩缩容后同键落到不同分片；分片函数对键做大小写不敏感处理，不同键被合并；窗口边界用闭区间，边界上的请求被双倍计算；单机额度用总额除以分片数并向上取整，总和超过总额；额度折算不考虑权重，热点分片被饿死；重新分配时余数随机分配，结果不可复现；余额不足时部分扣减，计数漂移；时钟偏移超限仍参与统计，一台机器可放大额度；策略校验不检查窗口为零，除零后全部放行；批量判定复用本地余额，第二个键命中第一个键的额度。dlimit/quota.py 是核心，tests/test_quota.py 15个红。要修的是：按上面的顺序把 10 个缺陷逐个修掉。

## 运行方式

在仓库根目录执行：

    python -m pytest tests -q

初始状态必须恰好是 15 failed，并且 15 个失败全部为 NotImplementedError；不能出现 collection、import 或 syntax error。实现 src/dlimit/quota.py 后应当变为 15 passed。

只调试一个用例：

    python -m pytest tests/test_quota.py -q -k window
    python -m pytest "tests/test_quota.py::test_defect_03_closed_window_double_counts_boundary" -q

## 字段表

| 字段 | 取值 |
| --- | --- |
| 所属领域 | 安全 |
| 子方向 | 漏洞检测与攻防 |
| 任务类型 | Bugfix |
| 语言/框架 | Python 3, pytest |
| 环境可复现等级 | 无外部依赖 |

## 文件结构

    sec-dist-limit/
    ├── README.md
    ├── pyproject.toml
    ├── src/
    │   └── dlimit/
    │       ├── __init__.py
    │       └── quota.py
    └── tests/
        ├── conftest.py
        └── test_quota.py

## src 布局说明

本仓库采用 src 布局：实现包位于 src/dlimit/，而不是仓库根目录下的 dlimit/。tests/conftest.py 会把仓库根的 src 目录插入 sys.path，因此测试可以直接导入 dlimit.quota，不需要先执行 pip install，也不会从仓库根误导入同名模块。pyproject.toml 只声明项目元数据和 pytest 配置，不引入运行期第三方依赖。

src/dlimit/quota.py 是唯一实现文件，恰好包含 8 个待实现函数。src/dlimit/__init__.py 只负责导出这些函数，不包含业务逻辑。tests/test_quota.py 恰好包含 15 个用例，前 10 个逐个对应题面中的缺陷，后 5 个覆盖额度守恒、窗口边界、扩缩容稳定性、时钟偏移排除和批量独立判定。

## 实现约定

- policy 字段：total 是全局总额度，shards 是分片数，window 是窗口长度，max_skew 是允许的最大时钟偏移，weights 是各分片权重；weights 全为零时按均权处理。可选字段 skew 是全局时钟偏移预留量，缺省为 0。
- state 字段：balance 是当前本地余额，events 是成功扣减的事件审计列表，元素含 at、key、cost。admit 只做整笔判定，余额或窗口额度不足时不得修改 balance 或 events。
- admit 返回 {"allowed": bool, "remaining": int, "key": str, "at": int}。它按 total、shards、skew 计算本地额度，并用 window_bounds 左开右闭区间统计当前窗口用量；余额不足或窗口用量加本次 cost 超过本地额度都整笔拒绝。成功时余额减去 cost 并向 events 追加一条记录；失败时余额和 events 全部保持原样。
- quota_batch 接收 [{"key": str, "cost": int, "balance": int}, ...]，每条请求使用自己的 balance 独立判定，返回 {key: {"allowed": bool, "remaining": int}}。同一批内的 key 不重复；不同批次之间不得共享余额。
- rebalance 返回 {分片下标: 额度}，总和必须等于 total。按权重比例向下取整，余数按小数部分从大到小、同分按分片下标从小到大确定性分配；不得使用随机数。权重全为零时退化为均分。
- local_quota 先在全局预留 skew，再对剩余额度做均分并向下取整，因此 local_quota 结果乘 shards 不超过 total。剩余额度不足或为负时返回 0。
- window_bounds 返回 (now - window, now)，语义是左开右闭区间 (start, end]；start 时刻的请求属于上一个窗口，end 时刻的请求只属于当前窗口。
- clock_skew_ok 在绝对偏移不超过 max_skew 时返回 True，超过时返回 False；max_skew 为负按 0 处理。
- validate_policy 按 shards、window、max_skew 的顺序返回问题码，可选值为 shards_missing、window_zero、max_skew_too_large。窗口为零时只返回问题码，绝不能做除法；max_skew 大于 window 视为偏移上限过大。
- 只允许 Python 标准库和 pytest；不联网、不使用随机数、不读取真实时钟。

## 约束

1. tests 不可修改：不得增删用例、不得改断言、不得跳过；只能修改 src/dlimit/quota.py。
2. 8 个函数的签名与 docstring 是验收契约，不要改签名，也不要改 src/dlimit/__init__.py 的导出用途。
3. 这是 src 布局，tests/conftest.py 负责路径；不要改成在仓库根放 dlimit/，也不要依赖安装后的包。
4. 初始的 15 个失败必须全部来自 NotImplementedError，不能靠抛别的异常或让测试收集失败来制造红。