"""中间件入口配置。"""

from dataclasses import dataclass


@dataclass(frozen=True)
class Config:
    """入口提供的并发与故障转移参数。

    client_share：单个客户端最多同时占用的在途分区调用数；None 表示不单独限制。
    backup_window：距查询绝对期限多少秒内，允许为没有证据的分区换备用端再试。
    """

    client_share: int | None = None
    backup_window: int = 2
