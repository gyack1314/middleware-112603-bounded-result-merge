"""聚合中间件语义测试：可证前缀、在途预算、游标与重启恢复。"""

import unittest

from aggregator import Aggregator, Config, MemoryStore


def make(**config_args):
    return Aggregator(MemoryStore(), Config(**config_args))


def declare(service, shard="a", node="one", revision=1, epoch=1, available=True, at=0):
    return service.source(
        at,
        {"shard": shard, "node": node, "revision": revision, "epoch": epoch, "available": available},
    )


def two_shard_service(until=50, partial=True, **config):
    service = make(**config)
    service.capacity(0, {"limit": 10})
    declare(service, "a", "one")
    declare(service, "b", "two")
    service.search(
        1,
        {"client": "c", "id": "q", "shards": ["a", "b"], "revision": 1, "until": until, "partial": partial},
    )
    service.drain()
    return service


def part(service, shard, node, rows, bound, attempt=1, epoch=1, at=2, query="q"):
    return service.part(
        at,
        {
            "id": query,
            "shard": shard,
            "node": node,
            "epoch": epoch,
            "attempt": attempt,
            "rows": rows,
            "bound": bound,
        },
    )


def page_results(events):
    return [event["record"] for event in events if event["kind"] == "page_result"]


def attempt_events(events, action=None):
    return [
        event["record"]
        for event in events
        if event["kind"] == "attempt" and (action is None or event["record"]["action"] == action)
    ]


class ProvablePrefixTests(unittest.TestCase):
    def test_entry_equal_to_bound_must_wait(self):
        service = two_shard_service()
        part(service, "a", "one", [[1, "a"], [2, "a"]], [5, "a"])
        part(service, "b", "two", [], [2, "0"])
        pending = service.page(4, {"id": "q", "client": "c", "limit": 3, "page": "p1"})
        self.assertEqual(pending["status"], "pending")  # (2,"a") 与下界 (2,"0") 并列，需等待
        part(service, "b", "two", [[2, "b"]], None, at=5)
        (answered,) = page_results(service.drain())
        self.assertEqual(answered["rows"], [[1, "a"], [2, "a"], [2, "b"]])
        self.assertFalse(answered["complete"])
        self.assertEqual(
            answered["missing"], [{"shard": "a", "reason": "bound_outstanding", "bound": [5, "a"]}]
        )

    def test_no_evidence_blocks_everything(self):
        service = two_shard_service()
        part(service, "a", "one", [[1, "a"]], None)
        pending = service.page(2, {"id": "q", "client": "c", "limit": 1, "page": "p"})
        self.assertEqual(pending["status"], "pending")  # b 尚未给出任何下界
        part(service, "b", "two", [], None, at=3)
        (answered,) = page_results(service.drain())
        self.assertEqual(answered["rows"], [[1, "a"]])
        self.assertTrue(answered["complete"])

    def test_bound_retreat_and_disorder_are_rejected(self):
        service = two_shard_service()
        part(service, "a", "one", [[1, "a"]], [5, "a"])
        self.assertEqual(
            part(service, "a", "one", [], [4, "a"], at=3)["reason"], "bound_retreat"
        )
        self.assertEqual(
            part(service, "a", "one", [[3, "x"]], [6, "a"], at=4)["reason"], "rows_below_bound"
        )
        self.assertEqual(
            part(service, "a", "one", [[7, "a"], [6, "b"]], [8, "a"], at=5)["reason"],
            "rows_unsorted",
        )
        info = service.inspect(6, {"id": "q"})
        self.assertEqual(info["shards"]["a"]["bound"], [5, "a"])  # 状态未被破坏

    def test_stale_report_is_ignored_and_keeps_accounting(self):
        service = two_shard_service()
        for bad in (
            {"attempt": 9},
            {"epoch": 9},
            {"node": "ghost"},
        ):
            fields = {"id": "q", "shard": "a", "node": "one", "epoch": 1, "attempt": 1,
                      "rows": [[1, "a"]], "bound": None}
            fields.update(bad)
            result = service.part(2, fields)
            self.assertEqual(result["accepted"], False)
            self.assertEqual(result["reason"], "stale_attempt")
        overview = service.inspect(3, {})
        self.assertEqual(overview["capacity"]["in_use"], 2)  # 迟报没有释放任何在途占用
        self.assertIsNone(service.inspect(3, {"id": "q"})["shards"]["a"]["bound"])

    def test_report_after_complete_is_rejected(self):
        service = two_shard_service()
        part(service, "a", "one", [], None)
        result = part(service, "a", "one", [], None, attempt=1, at=3)
        self.assertEqual(result["reason"], "stale_attempt")  # 原调用已随完结确认结束


class DeadlineTests(unittest.TestCase):
    def test_partial_page_lists_missing_bounds(self):
        service = two_shard_service(until=5)
        part(service, "a", "one", [[1, "a"]], [3, "a"])
        pending = service.page(3, {"id": "q", "client": "c", "limit": 2, "page": "p"})
        self.assertEqual(pending["status"], "pending")
        service.inspect(5, {"id": "q"})  # 推进到期限
        (answered,) = page_results(service.drain())
        self.assertEqual(answered["status"], "partial")
        self.assertEqual(answered["rows"], [])
        self.assertFalse(answered["complete"])  # 部分成功页不能表示查询完成
        self.assertEqual(
            answered["missing"],
            [
                {"shard": "a", "reason": "bound_outstanding", "bound": [3, "a"]},
                {"shard": "b", "reason": "awaiting_evidence"},
            ],
        )

    def test_timeout_when_partial_not_allowed_then_late_evidence_fills(self):
        service = two_shard_service(until=5, partial=False)
        service.page(3, {"id": "q", "client": "c", "limit": 2, "page": "p"})
        service.inspect(5, {"id": "q"})
        (timed_out,) = page_results(service.drain())
        self.assertEqual(timed_out["status"], "timeout")
        self.assertEqual(timed_out["rows"], [])
        # 迟来的分区结果仍可用于之后的取页
        part(service, "a", "one", [[1, "a"]], None, at=6)
        part(service, "b", "two", [[2, "b"]], None, at=7)
        answered = service.page(8, {"id": "q", "client": "c", "limit": 2, "page": "p"})
        self.assertEqual(answered["status"], "ok")
        self.assertEqual(answered["rows"], [[1, "a"], [2, "b"]])
        self.assertTrue(answered["complete"])

    def test_late_part_extends_next_page_without_rewriting_prefix(self):
        service = two_shard_service(until=5)
        part(service, "a", "one", [[1, "a"]], [9, "a"])
        part(service, "b", "two", [], [2, "0"], at=2)
        service.page(3, {"id": "q", "client": "c", "limit": 3, "page": "p1"})
        service.inspect(5, {"id": "q"})
        (partial,) = page_results(service.drain())
        self.assertEqual(partial["rows"], [[1, "a"]])
        service.ack(6, {"id": "q", "client": "c", "page": "p1"})
        part(service, "b", "two", [[2, "b"]], None, at=7)  # 迟报进入下一页
        answered = service.page(8, {"id": "q", "client": "c", "limit": 3, "page": "p2"})
        self.assertEqual(answered["rows"], [[2, "b"]])


class CursorAndReplayTests(unittest.TestCase):
    def answered_page(self, service):
        part(service, "a", "one", [[1, "a"], [2, "a"]], [5, "a"])
        part(service, "b", "two", [], None, at=3)
        response = service.page(4, {"id": "q", "client": "c", "limit": 2, "page": "p1"})
        self.assertEqual(response["status"], "ok")
        return response

    def test_unacked_page_replays_identically_after_restart(self):
        store = MemoryStore()
        service = Aggregator(store, Config())
        service.capacity(0, {"limit": 10})
        declare(service, "a", "one")
        declare(service, "b", "two")
        service.search(
            1, {"client": "c", "id": "q", "shards": ["a", "b"], "revision": 1, "until": 50, "partial": True}
        )
        sent = self.answered_page(service)
        service.drain()
        recovered = Aggregator(store.reopen(), Config())  # 服务重启
        replay = recovered.page(5, {"id": "q", "client": "c", "limit": 2, "page": "p1"})
        self.assertTrue(replay["replay"])
        self.assertEqual(replay["rows"], sent["rows"])  # 内容与条目顺序保持
        self.assertEqual(recovered.inspect(5, {"id": "q"})["unacked_page"], "p1")

    def test_page_size_change_does_not_resize_sent_page(self):
        service = two_shard_service()
        self.answered_page(service)
        replay = service.page(5, {"id": "q", "client": "c", "limit": 9, "page": "p1"})
        self.assertEqual(len(replay["rows"]), 2)  # 旧确认不被绑到新页大小

    def test_new_page_id_while_unacked_is_pointed_to_outstanding(self):
        service = two_shard_service()
        sent = self.answered_page(service)
        result = service.page(5, {"id": "q", "client": "c", "limit": 2, "page": "p2"})
        self.assertEqual(result["status"], "unacked")
        self.assertEqual(result["page"], "p1")
        self.assertEqual(result["rows"], sent["rows"])

    def test_ack_commits_cursor_and_next_page_continues(self):
        service = two_shard_service()
        self.answered_page(service)
        ack = service.ack(5, {"id": "q", "client": "c", "page": "p1"})
        self.assertEqual(ack["confirmed"], 2)
        part(service, "a", "one", [[5, "a"]], None, at=6)  # 行不早于已报下界 [5,"a"]
        answered = service.page(7, {"id": "q", "client": "c", "limit": 2, "page": "p2"})
        self.assertEqual(answered["rows"], [[5, "a"]])  # 不重复已确认条目
        self.assertTrue(answered["complete"])
        closed = service.ack(8, {"id": "q", "client": "c", "page": "p2"})
        self.assertTrue(closed["closed"])

    def test_consumer_cursor_survives_restart(self):
        store = MemoryStore()
        service = Aggregator(store, Config())
        service.capacity(0, {"limit": 10})
        declare(service, "a", "one")
        declare(service, "b", "two")
        service.search(
            1, {"client": "c", "id": "q", "shards": ["a", "b"], "revision": 1, "until": 50, "partial": True}
        )
        self.answered_page(service)
        service.ack(5, {"id": "q", "client": "c", "page": "p1"})
        recovered = Aggregator(store.reopen(), Config())
        info = recovered.inspect(6, {"id": "q"})
        self.assertEqual(info["confirmed"], 2)
        self.assertEqual(info["cursor"], [2, "a"])
        part(recovered, "a", "one", [[5, "a"]], None, at=7)
        answered = recovered.page(8, {"id": "q", "client": "c", "limit": 2, "page": "p2"})
        self.assertEqual(answered["rows"], [[5, "a"]])

    def test_ack_validation(self):
        service = two_shard_service()
        self.assertEqual(service.ack(2, {"id": "q", "client": "c", "page": "p1"})["error"],
                         "no_unacked_page")
        self.answered_page(service)
        mismatch = service.ack(5, {"id": "q", "client": "c", "page": "other"})
        self.assertEqual(mismatch["error"], "page_mismatch")
        self.assertEqual(mismatch["unacked"], "p1")


class RevisionAndFailoverTests(unittest.TestCase):
    def test_new_generation_without_revision_is_incomplete_not_reread(self):
        service = make()
        service.capacity(0, {"limit": 3})
        declare(service, "a", "old", revision=1)
        declare(service, "a", "new", revision=2)
        service.search(
            1, {"client": "c", "id": "q", "shards": ["a"], "revision": 1, "until": 5, "partial": True}
        )
        declare(service, "a", "old", revision=1, epoch=2, available=False, at=2)
        info = service.inspect(3, {"id": "q"})
        shard = info["shards"]["a"]
        self.assertEqual(shard["status"], "unavailable_at_revision")
        self.assertEqual(shard["attempts"][0]["end_reason"], "source_unavailable")
        nodes = {attempt["node"] for attempt in shard["attempts"]}
        self.assertNotIn("new", nodes)  # 没有改读修订 2 的新索引

    def test_backup_near_deadline_and_old_occupancy_held(self):
        service = make(backup_window=2)
        service.capacity(0, {"limit": 3})
        declare(service, "a", "one")
        declare(service, "a", "two")
        service.search(
            1, {"client": "c", "id": "q", "shards": ["a"], "revision": 1, "until": 10, "partial": True}
        )
        service.drain()
        service.inspect(8, {"id": "q"})  # 进入期限窗口
        opened = attempt_events(service.drain(), "open")
        self.assertEqual([(a["node"], a["attempt"]) for a in opened], [("two", 2)])
        info = service.inspect(8, {"id": "q"})
        states = [a["state"] for a in info["shards"]["a"]["attempts"]]
        self.assertEqual(states, ["open", "open"])  # 前一尝试的占用仍未确认结束
        self.assertEqual(service.inspect(8, {})["capacity"]["in_use"], 2)
        declare(service, "a", "one", epoch=2, at=9)  # 来源重宣告，确认旧占用结束
        ended = attempt_events(service.drain(), "end")
        self.assertEqual([(a["attempt"], a["reason"]) for a in ended], [(1, "epoch_changed")])
        # 同节点新代次仍可读修订 1：期限窗口内被用作备用端再试
        info = service.inspect(9, {"id": "q"})
        states = [(a["node"], a["epoch"], a["state"]) for a in info["shards"]["a"]["attempts"]]
        self.assertEqual(
            states, [("one", 1, "ended"), ("two", 1, "open"), ("one", 2, "open")]
        )
        self.assertEqual(service.inspect(9, {})["capacity"]["in_use"], 2)

    def test_backup_requires_same_revision(self):
        service = make(backup_window=2)
        service.capacity(0, {"limit": 3})
        declare(service, "a", "one")
        declare(service, "a", "two", revision=2)
        service.search(
            1, {"client": "c", "id": "q", "shards": ["a"], "revision": 1, "until": 10, "partial": True}
        )
        service.drain()
        service.inspect(9, {"id": "q"})
        self.assertEqual(attempt_events(service.drain(), "open"), [])  # 没有同修订备用端


class CapacityTests(unittest.TestCase):
    def test_total_limit_and_client_share(self):
        service = make(client_share=1)
        service.capacity(0, {"limit": 2})
        declare(service, "a", "one")
        declare(service, "b", "two")
        service.search(
            1, {"client": "c1", "id": "q1", "shards": ["a"], "revision": 1, "until": 50, "partial": True}
        )
        service.search(
            2, {"client": "c1", "id": "q2", "shards": ["b"], "revision": 1, "until": 50, "partial": True}
        )
        service.search(
            3, {"client": "c2", "id": "q3", "shards": ["b"], "revision": 1, "until": 50, "partial": True}
        )
        self.assertEqual(service.inspect(4, {"id": "q2"})["shards"]["b"]["status"], "not_started")
        self.assertEqual(
            service.inspect(4, {"id": "q2"})["shards"]["b"]["detail"], "有可用来源，等待在途额度"
        )
        self.assertEqual(
            service.inspect(4, {"id": "q3"})["shards"]["b"]["status"], "awaiting_evidence"
        )
        # q1 完结并关闭后，c1 的份额空出，q2 的调用得以开始
        part(service, "a", "one", [], None, at=5, query="q1")
        answered = service.page(6, {"id": "q1", "client": "c1", "limit": 1, "page": "p"})
        self.assertTrue(answered["complete"])
        service.ack(7, {"id": "q1", "client": "c1", "page": "p"})
        self.assertEqual(
            service.inspect(8, {"id": "q2"})["shards"]["b"]["status"], "awaiting_evidence"
        )

    def test_late_report_does_not_free_capacity_for_other_client(self):
        service = make()
        service.capacity(0, {"limit": 1})
        declare(service, "a", "one")
        declare(service, "b", "two")
        service.search(
            1, {"client": "c1", "id": "q1", "shards": ["a"], "revision": 1, "until": 50, "partial": True}
        )
        service.search(
            2, {"client": "c2", "id": "q2", "shards": ["b"], "revision": 1, "until": 50, "partial": True}
        )
        stale = service.part(
            3, {"id": "q1", "shard": "a", "node": "one", "epoch": 1, "attempt": 7,
                "rows": [], "bound": None}
        )
        self.assertEqual(stale["reason"], "stale_attempt")
        self.assertEqual(service.inspect(4, {})["capacity"]["in_use"], 1)
        self.assertEqual(service.inspect(4, {"id": "q2"})["shards"]["b"]["status"], "not_started")
        # 真实的完结证据才释放占用
        part(service, "a", "one", [], None, at=5, query="q1")
        self.assertEqual(
            service.inspect(6, {"id": "q2"})["shards"]["b"]["status"], "awaiting_evidence"
        )


class InspectTests(unittest.TestCase):
    def test_slow_partition_states_are_distinguishable(self):
        service = make()
        declare(service, "a", "one")
        service.search(
            1,
            {"client": "c", "id": "q", "shards": ["a", "b", "c"], "revision": 1, "until": 50,
             "partial": True},
        )
        info = service.inspect(2, {"id": "q"})
        self.assertEqual(info["shards"]["a"]["status"], "not_started")  # 容量未下发
        self.assertEqual(info["shards"]["a"]["detail"], "有可用来源，等待在途额度")
        self.assertEqual(info["shards"]["b"]["status"], "not_started")
        self.assertEqual(info["shards"]["b"]["detail"], "尚无来源宣告")
        self.assertEqual(info["shards"]["c"]["status"], "not_started")
        service.capacity(3, {"limit": 3})
        declare(service, "b", "two", at=3)
        declare(service, "c", "three", revision=2, at=3)  # 只有修订 2
        info = service.inspect(4, {"id": "q"})
        self.assertEqual(info["shards"]["a"]["status"], "awaiting_evidence")
        self.assertEqual(info["shards"]["b"]["status"], "awaiting_evidence")
        self.assertEqual(info["shards"]["c"]["status"], "unavailable_at_revision")
        part(service, "a", "one", [], [7, "a"], at=5)
        part(service, "b", "two", [], None, at=5)
        info = service.inspect(6, {"id": "q"})
        self.assertEqual(info["shards"]["a"]["status"], "evidence")
        self.assertEqual(info["shards"]["b"]["status"], "complete")


class ContractTests(unittest.TestCase):
    def test_duplicate_search_and_wrong_client(self):
        service = two_shard_service()
        again = service.search(
            2, {"client": "c", "id": "q", "shards": ["a"], "revision": 1, "until": 9, "partial": True}
        )
        self.assertEqual(again["error"], "duplicate_query")
        self.assertEqual(
            service.page(3, {"id": "q", "client": "other", "limit": 1, "page": "p"})["error"],
            "wrong_client",
        )
        self.assertEqual(
            service.ack(3, {"id": "q", "client": "other", "page": "p"})["error"], "wrong_client"
        )

    def test_sort_rule_must_be_the_supported_one(self):
        service = make()
        result = service.search(
            1,
            {"client": "c", "id": "q", "shards": ["a"], "revision": 1, "until": 9,
             "partial": True, "sort": {"rank": "desc", "key": "asc"}},
        )
        self.assertIn("不支持的排序规则", result["error"])
        ok = service.search(
            2,
            {"client": "c", "id": "q2", "shards": ["a"], "revision": 1, "until": 9,
             "partial": True, "sort": {"rank": "asc", "key": "asc"}},
        )
        self.assertTrue(ok["ok"])

    def test_structural_errors_are_not_journaled(self):
        store = MemoryStore()
        service = Aggregator(store, Config())
        bad = service.search(1, {"client": "c", "id": "q", "shards": [], "revision": 1,
                                 "until": 9, "partial": True})
        self.assertFalse(bad["ok"])
        self.assertEqual(list(store.events()), [])  # 未通过校验的输入视为从未发生


if __name__ == "__main__":
    unittest.main()
