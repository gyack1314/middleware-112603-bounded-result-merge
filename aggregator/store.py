"""输入事件日志：中间件状态完全由已接受的输入重放得到。"""

import json
from pathlib import Path


class MemoryStore:
    """内存日志，便于测试；restart 时内容保留、服务对象重建。"""

    def __init__(self, events=None):
        self._events = list(events) if events else []

    def append(self, event):
        self._events.append(event)

    def events(self):
        return iter(list(self._events))

    def reopen(self):
        return self


class FileStore:
    """追加式 JSONL 日志，可跨进程重启恢复。"""

    def __init__(self, path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._handle = self.path.open("a", encoding="utf-8")

    def append(self, event):
        self._handle.write(json.dumps(event, ensure_ascii=False) + "\n")
        self._handle.flush()

    def events(self):
        def read():
            if not self.path.exists():
                return
            with self.path.open(encoding="utf-8") as source:
                for line in source:
                    line = line.strip()
                    if line:
                        yield json.loads(line)

        return read()

    def reopen(self):
        return FileStore(self.path)
