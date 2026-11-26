# 多分区作业结果的有界合并与前缀承诺

rank 越小越靠前，同 rank 时按 key 字典序。bound 是分区对所有未交付项的下界，null 表示该分区本次结果已经完结。

## 输入消息

每个 JSONL 文件是一段独立输入记录。每行包括整数秒点 at、消息名称 kind 与对象 record。at 代表中间件收到消息的时刻，同一秒内按行序提交。业务时间和对应回执引用写在 record 中。

- **source**　分区 shard 对 node 宣告 revision、epoch 与 available。
- **capacity**　协调器设置 limit。
- **search**　client 提交 id、shards、revision、until 及 partial。
- **part**　node/epoch 为 id/shard 回报 rows 与 bound，attempt 建立实际来源。 rows 与 bound 都按 (rank, key) 表示。bound 含该位置本身，等于下界的下一条需要等待能否确定并列。
- **page**　client 为 id 请求 limit 条，page 为客户端申请标识。
- **ack**　client 确认 page 已消费。
- **restart**　服务重启。
- **inspect**　按 id 查询。

## 示例

- bound-with-ties.jsonl
- backup-changes-revision.jsonl

## 读取演练资料

运行环境为 Python 3.11，只使用标准库。在根目录运行以下命令可核对文件格式及读取接口。

    python3 scenario_reader.py examples
    python3 -m unittest discover -v

资料读取器会按文件统计各类参与方消息的数量。演练记录只描述各参与方交给系统的输入。
