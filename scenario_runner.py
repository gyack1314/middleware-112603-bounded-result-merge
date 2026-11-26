"""把演练记录驱动到聚合中间件上，输出处理流水。

用法：
    python3 scenario_runner.py examples/bound-with-ties.jsonl
    python3 scenario_runner.py examples --store-dir /tmp/aggregator-journals
"""

import argparse
import json
from pathlib import Path

from aggregator import Aggregator, Config, FileStore, MemoryStore
from scenario_reader import iter_messages

DEFAULT_KINDS = {"ack", "capacity", "inspect", "page", "part", "restart", "search", "source"}


def supported_kinds(path):
    registry = path.parent / "message_types.json" if path.is_file() else path / "message_types.json"
    if registry.exists():
        return set(json.loads(registry.read_text(encoding="utf-8")))
    return DEFAULT_KINDS


def run_scenario(path, *, store=None, config=None):
    """按到达顺序处理一段演练记录，返回处理流水（可 JSON 序列化的字典列表）。"""
    store = store if store is not None else MemoryStore()
    config = config or Config()
    service = Aggregator(store, config)
    transcript = []

    def drain():
        transcript.extend(service.drain())

    for message in iter_messages(path, supported_kinds(path)):
        at, kind, record = message["at"], message["kind"], message["record"]
        if kind == "restart":
            # 服务重启：日志保留，内存状态由重放恢复
            service = Aggregator(store.reopen(), config)
            transcript.append(
                {"at": at, "kind": "restart_result", "record": {"ok": True, "recovered": service.query_ids()}}
            )
            continue
        result = getattr(service, kind)(at, record)
        if result is not None:
            transcript.append({"at": at, "kind": f"{kind}_result", "record": result})
        drain()
    service.finish()
    drain()
    return transcript


def main(argv=None):
    parser = argparse.ArgumentParser(description="驱动本地聚合中间件演练")
    parser.add_argument("path", type=Path, help="场景文件或目录")
    parser.add_argument("--store-dir", type=Path, default=None, help="事件日志目录（默认内存）")
    parser.add_argument("--client-share", type=int, default=None, help="单客户端在途调用份额")
    parser.add_argument("--backup-window", type=int, default=2, help="距期限多少秒内允许换备用端")
    args = parser.parse_args(argv)
    config = Config(client_share=args.client_share, backup_window=args.backup_window)
    paths = sorted(args.path.glob("*.jsonl")) if args.path.is_dir() else [args.path]
    if not paths:
        parser.error("没有可读取的场景")
    for path in paths:
        if args.store_dir is not None:
            journal = args.store_dir / f"{path.stem}.journal.jsonl"
            journal.unlink(missing_ok=True)
            store = FileStore(journal)
        else:
            store = MemoryStore()
        for line in run_scenario(path, store=store, config=config):
            print(json.dumps(line, ensure_ascii=False))


if __name__ == "__main__":
    main()
