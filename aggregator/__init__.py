"""跨分区检索的请求聚合中间件。

协调分区预算、可证排序前缀与客户端游标，只交付可证明的全局有序前缀。
"""

from .config import Config
from .service import Aggregator
from .store import FileStore, MemoryStore

__all__ = ["Aggregator", "Config", "FileStore", "MemoryStore"]
