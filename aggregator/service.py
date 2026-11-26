"""跨分区检索的请求聚合中间件。

协调三处状态，只交付真正确定的结果：

- 分区预算：每次分区调用（attempt）独立占用一份在途资源，受总并发与客户端份额约束；
- 可证排序前缀：条目严格小于每个未完结分区的下界（bound）时才可交付，
  下界含该位置本身，等于下界的条目需要等待能否确定并列；
- 客户端游标：页一旦发出内容即固定，ack 之后才推进已确认位置，
  重取未确认的页保持原内容与条目顺序。

所有已接受的输入都写入事件日志，服务重启后按日志重放恢复。
"""

from __future__ import annotations

from bisect import insort
from dataclasses import dataclass, field

from .config import Config

DEFAULT_SORT = {"rank": "asc", "key": "asc"}

# 分片证据状态（值班端口径）
NOT_STARTED = "not_started"  # 未开始：还没有在途调用
AWAITING_EVIDENCE = "awaiting_evidence"  # 已接但没出证据
EVIDENCE = "evidence"  # 已回报下界，未完结
COMPLETE = "complete"  # 本次结果已完结（bound 为 null）
UNAVAILABLE = "unavailable_at_revision"  # 无法在本修订提供结果


def _is_int(value):
    return isinstance(value, int) and not isinstance(value, bool)


def check_key(value):
    """校验并规范化 (rank, key) 位置，返回可比较的元组。"""
    if not isinstance(value, (list, tuple)) or len(value) != 2:
        raise ValueError("位置需要 [rank, key] 两项")
    rank, key = value
    if isinstance(rank, bool) or not isinstance(rank, (int, float)):
        raise ValueError("rank 应为数字")
    if not isinstance(key, str):
        raise ValueError("key 应为字符串")
    return (rank, key)


@dataclass
class Attempt:
    """一次分区调用：在确认结束前一直占有一份在途资源。"""

    shard: str
    node: str
    epoch: int
    number: int
    opened_at: int
    state: str = "open"  # open | ended
    evidence: bool = False
    ended_at: int | None = None
    end_reason: str | None = None

    def info(self):
        return {
            "node": self.node,
            "epoch": self.epoch,
            "attempt": self.number,
            "state": self.state,
            "opened_at": self.opened_at,
            "ended_at": self.ended_at,
            "end_reason": self.end_reason,
            "evidence": self.evidence,
        }


@dataclass
class ShardState:
    """一个分区在一次查询中的证据与调用轨迹。"""

    name: str
    bound: tuple | None = None  # 最近一份有效下界；None 表示尚未回报
    complete: bool = False  # 已收到 null 下界
    reported: int = 0  # 累计接受的行数
    attempts: list = field(default_factory=list)
    next_number: int = 1

    def open_attempts(self):
        return [a for a in self.attempts if a.state == "open"]


@dataclass
class Query:
    """一次跨分区检索的全部聚合状态。"""

    id: str
    client: str
    shards: list
    revision: int
    until: int
    partial: bool
    sort: dict
    created_at: int
    state: str = "open"  # open | closed
    shard_states: dict = field(default_factory=dict)
    buffer: list = field(default_factory=list)  # 已回报、未确认的条目，按 (rank, key) 有序
    buffer_set: set = field(default_factory=set)
    sent_through: tuple | None = None  # 已发出（未必确认）的最后位置
    confirmed_through: tuple | None = None  # 客户端已确认的最后位置
    confirmed_count: int = 0
    unacked: dict | None = None  # {"page", "response"}：已发出待确认的页
    pending: dict | None = None  # {"page", "limit"}：尚未应答的取页请求


class Aggregator:
    """请求聚合中间件。每个公开方法对应一种输入消息，返回可 JSON 序列化的结果。"""

    def __init__(self, store, config=None):
        self.store = store
        self.config = config or Config()
        self.limit = 0  # 总并发，由 capacity 设置
        self.sources = {}  # (shard, node) -> 来源宣告
        self.queries = {}  # id -> Query
        self.now = 0
        self._out = []
        self._replaying = True
        for event in self.store.events():
            self._advance(event["at"])
            self._handle(event["event"], event["at"], event["record"])
        self._replaying = False

    # ------------------------------------------------------------------
    # 对外接口：发查询、分区提供证据、取页、确认、来源与容量、值班查询
    # ------------------------------------------------------------------

    def capacity(self, at, record):
        return self._accept("capacity", at, record)

    def source(self, at, record):
        return self._accept("source", at, record)

    def search(self, at, record):
        return self._accept("search", at, record)

    def part(self, at, record):
        return self._accept("part", at, record)

    def page(self, at, record):
        return self._accept("page", at, record)

    def ack(self, at, record):
        return self._accept("ack", at, record)

    def inspect(self, at, record):
        return self._accept("inspect", at, record)

    def finish(self):
        """输入耗尽后，把仍在等待的取页请求推进到各自期限结算。"""
        horizon = max(
            (q.until for q in self.queries.values() if q.state == "open" and q.pending is not None),
            default=None,
        )
        if horizon is not None and horizon > self.now:
            self._advance(horizon)

    def drain(self):
        """取出处理过程中产生的旁路输出（页应答、调用开闭等）。"""
        out, self._out = self._out, []
        return out

    def query_ids(self):
        return list(self.queries)

    # ------------------------------------------------------------------
    # 接收与重放
    # ------------------------------------------------------------------

    def _accept(self, kind, at, record):
        if not _is_int(at) or at < 0:
            return {"ok": False, "error": "at 应为非负整数秒点"}
        if at < self.now:
            return {"ok": False, "error": "time_regression"}
        if not isinstance(record, dict):
            return {"ok": False, "error": "record 应为对象"}
        error = self._validate(kind, record)
        if error is not None:
            return {"ok": False, "error": error}
        self.store.append({"event": kind, "at": at, "record": record})
        self._advance(at)
        return self._handle(kind, at, record)

    def _handle(self, kind, at, record):
        handler = {
            "capacity": self._do_capacity,
            "source": self._do_source,
            "search": self._do_search,
            "part": self._do_part,
            "page": self._do_page,
            "ack": self._do_ack,
            "inspect": self._do_inspect,
        }[kind]
        return handler(at, record)

    def _emit(self, kind, record):
        if not self._replaying:
            self._out.append({"at": self.now, "kind": kind, "record": record})

    def _advance(self, now):
        if now < self.now:
            return
        self.now = now
        self._ensure_all()
        self._maybe_backups()
        self._resolve_deadlines()

    # ------------------------------------------------------------------
    # 结构校验：不通过的输入不写入日志，视为从未发生
    # ------------------------------------------------------------------

    def _validate(self, kind, record):
        try:
            if kind == "capacity":
                self._need_int(record, "limit", 0)
            elif kind == "source":
                self._need_str(record, "shard")
                self._need_str(record, "node")
                self._need_int(record, "revision", 0)
                self._need_int(record, "epoch", 0)
                if not isinstance(record.get("available"), bool):
                    raise ValueError("available 应为布尔")
            elif kind == "search":
                self._need_str(record, "client")
                self._need_str(record, "id")
                shards = record.get("shards")
                if (
                    not isinstance(shards, list)
                    or not shards
                    or any(not isinstance(s, str) or not s for s in shards)
                ):
                    raise ValueError("shards 应为非空字符串列表")
                if len(set(shards)) != len(shards):
                    raise ValueError("shards 不应重复")
                self._need_int(record, "revision", 0)
                self._need_int(record, "until", 0)
                if not isinstance(record.get("partial"), bool):
                    raise ValueError("partial 应为布尔")
                if record.get("sort", DEFAULT_SORT) != DEFAULT_SORT:
                    raise ValueError("不支持的排序规则，仅支持 rank 升序、key 字典序")
            elif kind == "part":
                self._need_str(record, "id")
                self._need_str(record, "shard")
                self._need_str(record, "node")
                self._need_int(record, "epoch", 0)
                self._need_int(record, "attempt", 1)
                if "rows" not in record or not isinstance(record["rows"], list):
                    raise ValueError("rows 应为列表")
                for row in record["rows"]:
                    check_key(row)
                if "bound" not in record:
                    raise ValueError("缺少 bound，完结时应显式给 null")
                if record["bound"] is not None:
                    check_key(record["bound"])
            elif kind == "page":
                self._need_str(record, "id")
                self._need_str(record, "client")
                self._need_str(record, "page")
                self._need_int(record, "limit", 1)
            elif kind == "ack":
                self._need_str(record, "id")
                self._need_str(record, "client")
                self._need_str(record, "page")
            elif kind == "inspect":
                if record.get("id") is not None:
                    self._need_str(record, "id")
            else:
                return f"未登记的消息名称 {kind}"
        except ValueError as error:
            return str(error)
        return None

    @staticmethod
    def _need_str(record, name):
        if not isinstance(record.get(name), str) or not record[name]:
            raise ValueError(f"{name} 应为非空字符串")

    @staticmethod
    def _need_int(record, name, minimum):
        if not _is_int(record.get(name)) or record[name] < minimum:
            raise ValueError(f"{name} 应为不小于 {minimum} 的整数")

    # ------------------------------------------------------------------
    # 容量与调用（attempt）生命周期
    # ------------------------------------------------------------------

    def _open_attempt_count(self):
        return sum(
            1
            for q in self.queries.values()
            for name in q.shards
            for a in q.shard_states[name].attempts
            if a.state == "open"
        )

    def _client_attempt_count(self, client):
        return sum(
            1
            for q in self.queries.values()
            if q.client == client
            for name in q.shards
            for a in q.shard_states[name].attempts
            if a.state == "open"
        )

    def _slot_available(self, query):
        if self._open_attempt_count() >= self.limit:
            return False
        share = self.config.client_share
        if share is not None and self._client_attempt_count(query.client) >= share:
            return False
        return True

    def _suitable_nodes(self, query, shard):
        return sorted(
            node
            for (s, node), decl in self.sources.items()
            if s == shard and decl["available"] and decl["revision"] == query.revision
        )

    def _open_attempt(self, query, state, node):
        decl = self.sources[(state.name, node)]
        attempt = Attempt(
            shard=state.name,
            node=node,
            epoch=decl["epoch"],
            number=state.next_number,
            opened_at=self.now,
        )
        state.next_number += 1
        state.attempts.append(attempt)
        self._emit(
            "attempt",
            {
                "action": "open",
                "id": query.id,
                "shard": state.name,
                "node": node,
                "epoch": decl["epoch"],
                "attempt": attempt.number,
            },
        )
        return attempt

    def _end_attempt(self, query, attempt, reason):
        attempt.state = "ended"
        attempt.ended_at = self.now
        attempt.end_reason = reason
        self._emit(
            "attempt",
            {
                "action": "end",
                "id": query.id,
                "shard": attempt.shard,
                "node": attempt.node,
                "epoch": attempt.epoch,
                "attempt": attempt.number,
                "reason": reason,
            },
        )

    def _ensure_attempt(self, query, state):
        if state.complete or state.open_attempts():
            return
        for node in self._suitable_nodes(query, state.name):
            if self._slot_available(query):
                self._open_attempt(query, state, node)
            return  # 有可用来源但额度不足时保持未开始，等待容量变化

    def _ensure_all(self):
        for query in self.queries.values():
            if query.state != "open":
                continue
            for name in query.shards:
                self._ensure_attempt(query, query.shard_states[name])

    def _maybe_backups(self):
        """接近期限时，为已接但没出证据的分区换同修订备用端再试。

        前一尝试保持 open，其实际占用要等来源重宣告或查询关闭来确认结束。
        """
        for query in self.queries.values():
            if query.state != "open":
                continue
            if self.now < query.until - self.config.backup_window:
                continue
            for name in query.shards:
                state = query.shard_states[name]
                if state.complete or state.bound is not None:
                    continue
                open_attempts = state.open_attempts()
                if not open_attempts:
                    continue  # 尚未开始的由 _ensure_attempt 负责
                busy = {a.node for a in open_attempts}
                for node in self._suitable_nodes(query, name):
                    if node in busy:
                        continue
                    if not self._slot_available(query):
                        break
                    self._open_attempt(query, state, node)
                    break

    # ------------------------------------------------------------------
    # 各类输入的处理
    # ------------------------------------------------------------------

    def _do_capacity(self, at, record):
        self.limit = record["limit"]
        self._ensure_all()
        return {"ok": True, "limit": self.limit, "in_use": self._open_attempt_count()}

    def _do_source(self, at, record):
        shard, node = record["shard"], record["node"]
        self.sources[(shard, node)] = {
            "revision": record["revision"],
            "epoch": record["epoch"],
            "available": record["available"],
        }
        ended = []
        for query in self.queries.values():
            if query.state != "open":
                continue
            state = query.shard_states.get(shard)
            if state is None:
                continue
            for attempt in state.open_attempts():
                if attempt.node != node:
                    continue
                if not record["available"]:
                    self._end_attempt(query, attempt, "source_unavailable")
                    ended.append(attempt.number)
                elif attempt.epoch != record["epoch"]:
                    self._end_attempt(query, attempt, "epoch_changed")
                    ended.append(attempt.number)
        self._ensure_all()
        return {"ok": True, "ended_attempts": ended}

    def _do_search(self, at, record):
        if record["id"] in self.queries:
            return {"ok": False, "error": "duplicate_query"}
        query = Query(
            id=record["id"],
            client=record["client"],
            shards=list(record["shards"]),
            revision=record["revision"],
            until=record["until"],
            partial=record["partial"],
            sort=record.get("sort", DEFAULT_SORT),
            created_at=at,
        )
        query.shard_states = {name: ShardState(name) for name in query.shards}
        self.queries[query.id] = query
        self._ensure_all()
        return {
            "ok": True,
            "id": query.id,
            "shards": {
                name: self._shard_status(query, query.shard_states[name])[0] for name in query.shards
            },
        }

    def _do_part(self, at, record):
        query = self.queries.get(record["id"])
        if query is None:
            return {"ok": True, "accepted": False, "reason": "unknown_query"}
        state = query.shard_states.get(record["shard"])
        if state is None:
            return {"ok": True, "accepted": False, "reason": "unknown_shard"}
        if query.state != "open":
            return {"ok": True, "accepted": False, "reason": "query_closed"}
        attempt = next(
            (
                a
                for a in state.open_attempts()
                if a.node == record["node"]
                and a.epoch == record["epoch"]
                and a.number == record["attempt"]
            ),
            None,
        )
        if attempt is None:
            # 迟报不属于任何在途调用：忽略，且不影响在途记账
            return {"ok": True, "accepted": False, "reason": "stale_attempt"}
        if state.complete:
            return {"ok": True, "accepted": False, "reason": "shard_complete"}
        rows = [check_key(row) for row in record["rows"]]
        if rows != sorted(rows):
            return {"ok": True, "accepted": False, "reason": "rows_unsorted"}
        if state.bound is not None and rows and rows[0] < state.bound:
            return {"ok": True, "accepted": False, "reason": "rows_below_bound"}
        bound = record["bound"]
        if bound is not None:
            bound = check_key(bound)
            floor = state.bound
            if rows and (floor is None or rows[-1] > floor):
                floor = rows[-1]
            if floor is not None and bound < floor:
                return {"ok": True, "accepted": False, "reason": "bound_retreat"}
        added = 0
        for row in rows:
            if row not in query.buffer_set:
                insort(query.buffer, row)
                query.buffer_set.add(row)
                added += 1
        state.reported += added
        attempt.evidence = True
        if bound is None:
            state.complete = True
            self._end_attempt(query, attempt, "evidence_complete")
        else:
            state.bound = bound
        self._try_pending(query)
        self._ensure_all()
        return {
            "ok": True,
            "accepted": True,
            "rows": added,
            "duplicates": len(rows) - added,
            "complete": state.complete,
        }

    def _do_page(self, at, record):
        query = self.queries.get(record["id"])
        if query is None:
            return {"ok": False, "error": "unknown_query"}
        if query.client != record["client"]:
            return {"ok": False, "error": "wrong_client"}
        if query.state != "open":
            return {
                "status": "ok",
                "id": query.id,
                "client": query.client,
                "page": record["page"],
                "rows": [],
                "complete": True,
                "missing": [],
            }
        if query.unacked is not None:
            stored = query.unacked["response"]
            if query.unacked["page"] == record["page"]:
                # 重取已发出但未确认的页：保持原内容与条目顺序
                replay = dict(stored)
                replay["replay"] = True
                return replay
            return {
                "status": "unacked",
                "id": query.id,
                "client": query.client,
                "page": query.unacked["page"],
                "requested": record["page"],
                "rows": stored["rows"],
                "complete": stored["complete"],
                "missing": stored["missing"],
            }
        if query.pending is not None:
            if query.pending["page"] == record["page"]:
                return {
                    "status": "pending",
                    "id": query.id,
                    "client": query.client,
                    "page": record["page"],
                }
            # 未应答过的请求不携带任何已交付内容，可以被新标识取代
            superseded = query.pending["page"]
            query.pending = {"page": record["page"], "limit": record["limit"]}
            return {
                "status": "pending",
                "id": query.id,
                "client": query.client,
                "page": record["page"],
                "superseded": superseded,
            }
        query.pending = {"page": record["page"], "limit": record["limit"]}
        if self._fillable(query):
            return self._answer(query, "filled")
        if self.now >= query.until:
            return self._answer(query, "deadline")
        return {"status": "pending", "id": query.id, "client": query.client, "page": record["page"]}

    def _do_ack(self, at, record):
        query = self.queries.get(record["id"])
        if query is None:
            return {"ok": False, "error": "unknown_query"}
        if query.client != record["client"]:
            return {"ok": False, "error": "wrong_client"}
        if query.unacked is None:
            return {"ok": False, "error": "no_unacked_page"}
        if query.unacked["page"] != record["page"]:
            return {"ok": False, "error": "page_mismatch", "unacked": query.unacked["page"]}
        response = query.unacked["response"]
        query.unacked = None
        rows = response["rows"]
        query.confirmed_count += len(rows)
        if rows:
            query.confirmed_through = tuple(rows[-1])
            while query.buffer and query.buffer[0] <= query.confirmed_through:
                query.buffer_set.discard(query.buffer.pop(0))
        closed = False
        if response["complete"]:
            query.state = "closed"
            closed = True
            for name in query.shards:
                for attempt in query.shard_states[name].open_attempts():
                    self._end_attempt(query, attempt, "query_closed")
            self._ensure_all()
        return {
            "ok": True,
            "page": record["page"],
            "confirmed": query.confirmed_count,
            "closed": closed,
        }

    def _do_inspect(self, at, record):
        if record.get("id") is None:
            return {
                "ok": True,
                "now": self.now,
                "capacity": {
                    "limit": self.limit,
                    "in_use": self._open_attempt_count(),
                    "client_share": self.config.client_share,
                    "clients": {
                        q.client: self._client_attempt_count(q.client)
                        for q in self.queries.values()
                    },
                },
                "queries": {
                    q.id: {"state": q.state, "client": q.client, "until": q.until}
                    for q in self.queries.values()
                },
            }
        query = self.queries.get(record["id"])
        if query is None:
            return {"ok": False, "error": "unknown_query"}
        open_query = query.state == "open"
        return {
            "ok": True,
            "id": query.id,
            "client": query.client,
            "revision": query.revision,
            "until": query.until,
            "partial": query.partial,
            "sort": query.sort,
            "state": query.state,
            "now": self.now,
            "confirmed": query.confirmed_count,
            "cursor": list(query.confirmed_through) if query.confirmed_through else None,
            "unacked_page": query.unacked["page"] if query.unacked else None,
            "pending_page": query.pending["page"] if query.pending else None,
            "deliverable": len(self._deliverable(query)) if open_query else 0,
            "missing": self._missing(query) if open_query else [],
            "shards": {
                name: self._shard_info(query, query.shard_states[name]) for name in query.shards
            },
        }

    # ------------------------------------------------------------------
    # 可证前缀与取页
    # ------------------------------------------------------------------

    def _deliverable(self, query):
        """当前可证明属于全局前缀、且尚未发出的条目。"""
        if query.sent_through is None:
            candidates = list(query.buffer)
        else:
            candidates = [row for row in query.buffer if row > query.sent_through]
        bound = None
        for name in query.shards:
            state = query.shard_states[name]
            if state.complete:
                continue
            if state.bound is None:
                return []  # 有分区尚未给出任何下界，无法证明任何前缀
            bound = state.bound if bound is None else min(bound, state.bound)
        if bound is None:
            return candidates  # 全部完结
        # 下界含该位置本身：等于下界的条目需要等待
        return [row for row in candidates if row < bound]

    def _fillable(self, query):
        pending = query.pending
        if pending is None:
            return False
        if all(query.shard_states[name].complete for name in query.shards):
            return True
        return len(self._deliverable(query)) >= pending["limit"]

    def _try_pending(self, query):
        if query.pending is not None and self._fillable(query):
            response = self._answer(query, "filled")
            self._emit("page_result", response)

    def _resolve_deadlines(self):
        for query in self.queries.values():
            if query.state == "open" and query.pending is not None and query.until <= self.now:
                response = self._answer(query, "deadline")
                self._emit("page_result", response)

    def _answer(self, query, why):
        pending = query.pending
        query.pending = None
        limit = pending["limit"]
        deliverable = self._deliverable(query)
        all_complete = all(query.shard_states[name].complete for name in query.shards)
        if len(deliverable) >= limit:
            rows, status = deliverable[:limit], "ok"
            complete = all_complete and len(deliverable) == limit
        elif all_complete:
            rows, status, complete = deliverable, "ok", True
        elif why == "deadline" and query.partial:
            # 期限到达：按调用方允许的口径交付已证明的前缀，并列出欠谁的下界
            rows, status, complete = deliverable, "partial", False
        elif why == "deadline":
            rows, status, complete = [], "timeout", False
        else:
            rows, status, complete = deliverable, "ok", all_complete
        response = {
            "status": status,
            "id": query.id,
            "client": query.client,
            "page": pending["page"],
            "rows": [list(row) for row in rows],
            "complete": complete,
            "missing": self._missing(query),
        }
        if status != "timeout":
            if rows:
                query.sent_through = rows[-1]
            query.unacked = {"page": pending["page"], "response": response}
        return response

    def _missing(self, query):
        """还欠哪些分区的下界，以及各自卡在哪一步。"""
        missing = []
        for name in query.shards:
            state = query.shard_states[name]
            if state.complete:
                continue
            if state.bound is not None:
                reason = "bound_outstanding"
            else:
                reason = self._shard_status(query, state)[0]
            entry = {"shard": name, "reason": reason}
            if state.bound is not None:
                entry["bound"] = list(state.bound)
            missing.append(entry)
        return missing

    # ------------------------------------------------------------------
    # 值班端视图
    # ------------------------------------------------------------------

    def _shard_status(self, query, state):
        if state.complete:
            return COMPLETE, "本次结果已完结"
        if state.bound is not None:
            return EVIDENCE, "已回报下界"
        if state.open_attempts():
            return AWAITING_EVIDENCE, "调用已被接受，尚未回报下界"
        declarations = [decl for (s, _), decl in self.sources.items() if s == state.name]
        if not declarations:
            return NOT_STARTED, "尚无来源宣告"
        if self._suitable_nodes(query, state.name):
            return NOT_STARTED, "有可用来源，等待在途额度"
        return UNAVAILABLE, f"没有可在修订 {query.revision} 提供结果的可用来源"

    def _shard_info(self, query, state):
        status, detail = self._shard_status(query, state)
        return {
            "status": status,
            "detail": detail,
            "bound": list(state.bound) if state.bound is not None else None,
            "complete": state.complete,
            "reported": state.reported,
            "attempts": [attempt.info() for attempt in state.attempts],
        }
