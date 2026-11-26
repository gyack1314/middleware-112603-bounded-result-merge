# 多分区作业结果的有界合并与前缀承诺

rank 越小越靠前，同 rank 时按 key 字典序。bound 是分区对所有未交付项的下界，null 表示该分区本次结果已经完结。

仓库包含两部分：

- `aggregator/`：请求聚合中间件。从分区预算、可证排序前缀和客户端游标三处协调，只交付真正确定的结果。
- `scenario_reader.py` 与 `examples/`：演练资料与读取器；`scenario_runner.py` 把演练记录驱动到中间件上，输出处理流水。

## 交付规则（可证排序前缀）

- 条目按 (rank, key) 排序，并列按业务主键 key；排序规则随查询提交（`sort`，当前仅支持 rank 升序、key 字典序）。
- bound 含该位置本身：等于下界的下一条需要等待能否确定并列。只有严格小于**每个**未完结分区下界的条目才可交付。
- 某分区尚未给出任何 bound 时，无法证明任何前缀，任何条目都不交付。
- 正常分区已回的记录可以暂留，直到它们进入可证前缀。
- 期限到达时，调用方在其允许的口径内拿到已证明的前缀（`partial: true` 时给 `partial` 页，否则给 `timeout`），并看到欠哪一份下界（`missing`）；部分成功页永远 `complete: false`，不表示全查询已完成。

## 分区预算与调用（attempt）

- 每次分区调用是一个 attempt，独立占用一份在途资源。总并发由 `capacity` 设置，单客户端份额由入口配置（`Config.client_share`）。
- 分区回报的 rows 与 bound 通过 attempt 与同次查询、源修订及分片代次（node/epoch）关联；不属于任何在途调用的迟报按 `stale_attempt` 忽略，不影响在途记账，也不会替别的客户端释放资源。
- 分区的下界只能前进：行不得早于已报下界，新下界不得回退，违反契约的回报被拒绝。
- 接近期限时（`Config.backup_window` 秒内），聚合器可为已接但没出证据的分区换同修订的备用端再试；前一尝试的实际占用保持记账，直到来源重宣告（代次变更或不可用）、自身流完结或查询关闭来确认结束。
- 新的分区代次若不再能读取同一修订，该分区处于未完成（`unavailable_at_revision`），而不是改读新索引。

## 取页、确认与游标

- `page` 是客户端申请标识：已发出但未确认的页，重取时保持其内容与条目顺序，页大小变化也不会把旧确认绑到新页。
- 仍有未确认页时换一个页标识，会得到 `unacked` 应答并指回未确认的页。
- `ack` 确认后游标才推进；迟来的分区结果可用于下一页，但不能重写已给的前缀。
- 已确认的完结页被 ack 后查询关闭，其在途调用的占用随之确认结束。
- 所有已接受的输入写入事件日志（`MemoryStore` / `FileStore`），服务重启后按日志重放恢复；消费者保留的读取位置（游标与未确认页）在重启后可继续。

## 值班查询（inspect）

`inspect` 按查询标识给出每个分区的状态，低速分区可区分为：

- `not_started`：未开始（尚无来源宣告，或有可用来源但在等待在途额度）；
- `awaiting_evidence`：调用已被接受，但还没出证据；
- `unavailable_at_revision`：没有可在本修订提供结果的可用来源；
- 另有 `evidence`（已回报下界）与 `complete`（已完结）。

不带标识的 `inspect` 给出全局容量与各查询概览。

## 输入消息

每个 JSONL 文件是一段独立输入记录。每行包括整数秒点 at、消息名称 kind 与对象 record。at 代表中间件收到消息的时刻，同一秒内按行序提交。业务时间和对应回执引用写在 record 中。

- **source**　分区 shard 对 node 宣告 revision、epoch 与 available。
- **capacity**　协调器设置 limit。
- **search**　client 提交 id、shards、revision、until 及 partial，可显式携带 sort。
- **part**　node/epoch 为 id/shard 回报 rows 与 bound，attempt 建立实际来源。 rows 与 bound 都按 (rank, key) 表示。bound 含该位置本身，等于下界的下一条需要等待能否确定并列。
- **page**　client 为 id 请求 limit 条，page 为客户端申请标识。
- **ack**　client 确认 page 已消费。
- **restart**　服务重启。
- **inspect**　按 id 查询。

## 示例

- bound-with-ties.jsonl　并列未决时等待、重启后重取未确认页、期限给部分前缀
- backup-changes-revision.jsonl　备用端换了修订，分区处于未完成而非改读新索引
- failover-and-capacity.jsonl　期限前换同修订备用端、旧占用确认结束、迟报被忽略

## 运行

运行环境为 Python 3.11，只使用标准库。在根目录运行以下命令可核对文件格式及读取接口。

    python3 scenario_reader.py examples
    python3 -m unittest discover -v

资料读取器会按文件统计各类参与方消息的数量。演练记录只描述各参与方交给系统的输入。

把演练记录驱动到中间件上，查看处理流水（页应答、调用开闭、值班视图）：

    python3 scenario_runner.py examples/bound-with-ties.jsonl
    python3 scenario_runner.py examples --store-dir /tmp/journals --client-share 2 --backup-window 2

中间件接口即四种可运行操作：`Aggregator.search`（发查询）、`Aggregator.part`（分区提供证据）、`Aggregator.page`（取页）、`Aggregator.ack`（确认），另有 `source`、`capacity`、`inspect` 与 `finish`。新的合成分区只需按上述消息契约产出 JSONL 记录即可接入演练。
