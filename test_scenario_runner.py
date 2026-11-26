"""端到端检查：三份演练记录经中间件处理后的关键流水。"""

import unittest
from pathlib import Path

from aggregator import Config, MemoryStore
from scenario_runner import run_scenario

EXAMPLES = Path(__file__).with_name("examples")


def records(transcript, kind):
    return [line["record"] for line in transcript if line["kind"] == kind]


class BoundWithTiesTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.transcript = run_scenario(EXAMPLES / "bound-with-ties.jsonl")

    def test_page_waits_until_tie_is_decidable(self):
        pages = records(self.transcript, "page_result")
        self.assertEqual(pages[0]["status"], "pending")  # p1 在并列未决时不发
        answered = pages[1]
        self.assertEqual(answered["rows"], [[1, "a"], [2, "a"], [2, "b"]])
        self.assertFalse(answered["complete"])

    def test_restart_replays_unacked_page_identically(self):
        self.assertEqual(records(self.transcript, "restart_result")[0]["recovered"], ["q"])
        pages = records(self.transcript, "page_result")
        sent, replay = pages[1], pages[2]
        self.assertTrue(replay["replay"])
        self.assertEqual(replay["rows"], sent["rows"])

    def test_final_page_is_partial_and_names_missing_bound(self):
        final = records(self.transcript, "page_result")[-1]
        self.assertEqual(final["status"], "partial")
        self.assertFalse(final["complete"])  # 部分成功页不表示查询完成
        self.assertEqual(
            final["missing"], [{"shard": "a", "reason": "bound_outstanding", "bound": [5, "a"]}]
        )


class BackupChangesRevisionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.transcript = run_scenario(EXAMPLES / "backup-changes-revision.jsonl")

    def test_partition_without_revision_is_unavailable_not_reread(self):
        opened = [a for a in records(self.transcript, "attempt") if a["action"] == "open"]
        self.assertEqual([a["node"] for a in opened], ["old"])  # 从未改读修订 2 的 new
        inspection = records(self.transcript, "inspect_result")[0]
        self.assertEqual(
            inspection["shards"]["a"]["status"], "unavailable_at_revision"
        )

    def test_page_settles_at_deadline_with_missing_bound(self):
        page = records(self.transcript, "page_result")[-1]  # 期限到达时的结算
        self.assertEqual(page["status"], "partial")
        self.assertEqual(page["rows"], [])
        self.assertEqual(page["missing"], [{"shard": "a", "reason": "unavailable_at_revision"}])


class FailoverAndCapacityTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.transcript = run_scenario(EXAMPLES / "failover-and-capacity.jsonl")

    def test_backup_attempt_opens_near_deadline_on_same_revision(self):
        opened = [a for a in records(self.transcript, "attempt") if a["action"] == "open"]
        self.assertEqual(
            [(a["shard"], a["node"], a["attempt"]) for a in opened],
            [("a", "one", 1), ("b", "three", 1), ("a", "two", 2)],
        )

    def test_previous_attempt_occupancy_ends_only_on_confirmation(self):
        ended = [a for a in records(self.transcript, "attempt") if a["action"] == "end"]
        self.assertIn(
            {"action": "end", "id": "q", "shard": "a", "node": "one", "epoch": 1,
             "attempt": 1, "reason": "query_closed"},
            ended,
        )
        mid = records(self.transcript, "inspect_result")[1]  # at 14，查询尚未关闭
        states = {a["attempt"]: a["state"] for a in mid["shards"]["a"]["attempts"]}
        self.assertEqual(states, {1: "open", 2: "ended"})

    def test_late_report_after_close_is_rejected(self):
        last_part = records(self.transcript, "part_result")[-1]
        self.assertEqual(last_part["accepted"], False)
        self.assertEqual(last_part["reason"], "query_closed")

    def test_query_closes_after_complete_page_is_acked(self):
        acks = records(self.transcript, "ack_result")
        self.assertEqual(acks[-1]["confirmed"], 4)
        self.assertTrue(acks[-1]["closed"])
        final = records(self.transcript, "inspect_result")[-1]
        self.assertEqual(final["state"], "closed")
        self.assertEqual(final["cursor"], [4, "x"])


class RunnerPropertyTests(unittest.TestCase):
    def test_all_examples_are_deterministic(self):
        for path in sorted(EXAMPLES.glob("*.jsonl")):
            with self.subTest(filename=path.name):
                first = run_scenario(path, store=MemoryStore(), config=Config())
                second = run_scenario(path, store=MemoryStore(), config=Config())
                self.assertEqual(first, second)

    def test_every_page_request_gets_an_answer(self):
        for path in sorted(EXAMPLES.glob("*.jsonl")):
            with self.subTest(filename=path.name):
                transcript = run_scenario(path)
                pages = records(transcript, "page_result")
                self.assertTrue(pages)
                self.assertNotEqual(pages[-1]["status"], "pending")


if __name__ == "__main__":
    unittest.main()
