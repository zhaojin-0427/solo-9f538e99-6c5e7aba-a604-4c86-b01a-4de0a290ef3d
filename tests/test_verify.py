# -*- coding: utf-8 -*-
"""核验功能测试（标准库 unittest）。

运行：TEST_SITE=/path/to/site-packages ./scripts/run_tests.sh
"""

import json
import os
import site
import sys
import tempfile
import unittest
from unittest import mock

for _p in os.environ.get("TEST_SITE", "").split(os.pathsep):
    if _p:
        site.addsitedir(_p)
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import collector, verify  # noqa: E402

ACTIVE = collector.ACTIVE_NAME
ROTATED = collector.ROTATED_NAME


def rec(seq: int, temp: float = 25.0, ts: str = "2026-11-15T08:00:00Z") -> str:
    return json.dumps({"seq": seq, "ts": ts, "temp": temp})


class VerifyTestBase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.data = os.path.join(self.tmp.name, "data")
        self.db = os.path.join(self.tmp.name, "db", "test.db")
        os.makedirs(self.data, exist_ok=True)
        self.active = os.path.join(self.data, ACTIVE)
        self.rotated = os.path.join(self.data, ROTATED)
        self.write(ACTIVE, "")

    def tearDown(self):
        self.tmp.cleanup()

    def write(self, name, text, mode="a"):
        with open(os.path.join(self.data, name), mode, encoding="utf-8") as fh:
            fh.write(text)

    def seed_collected(self, seqs, temp=25.0):
        self.write(ACTIVE, "".join(rec(s, temp + s) + "\n" for s in seqs))
        result = collector.collect_once(self.data, self.db)
        self.assertEqual(result["status"], "ok", result["error"])
        return result

    def reports(self):
        return verify.list_reports(self.db)

    def seqs(self):
        rows, total = collector.list_records(self.db)
        return sorted(r["seq"] for r in rows), total

    def item_by_seq(self, report, seq):
        return next(x for x in report["items"] if x["seq"] == seq)


class VerificationCoreTest(VerifyTestBase):
    def test_all_match_records_watermark_and_snapshot(self):
        self.seed_collected([1, 2, 3])
        report = verify.run_verification(self.db, self.data, "1", "3")

        self.assertEqual(report["summary"], {"total": 3, "match": 3, "mismatch": 0, "missing": 0})
        self.assertEqual([x["seq"] for x in report["items"]], [1, 2, 3])
        for item in report["items"]:
            self.assertEqual(item["verdict"], verify.VERDICT_MATCH)
            self.assertEqual(item["log_raw"], item["db_raw"])

        # 本次入库水位
        self.assertEqual(
            report["watermark"], {"seq": 3, "record_id": 3, "count": 3}
        )
        # 两份日志截面（active 存在、rotated 不存在）
        files = {f["name"]: f for f in report["snapshot"]["files"]}
        self.assertEqual(set(files), {ACTIVE, ROTATED})
        self.assertTrue(files[ACTIVE]["present"])
        self.assertEqual(files[ACTIVE]["size"], os.path.getsize(self.active))
        self.assertEqual(files[ACTIVE]["sha256"], verify._capture_file(self.data, ACTIVE).sha256)
        self.assertFalse(files[ROTATED]["present"])
        self.assertIsNone(files[ROTATED]["sha256"])
        self.assertEqual(report["db_gap_count"], 0)

    def test_same_seq_different_content_is_mismatch_not_match(self):
        self.seed_collected([1, 2, 3])
        # 日志中同一 seq 同时有“与库内一致”和“不一致”的完整行：不能判一致
        self.write(ACTIVE, rec(2, 99.9) + "\n")

        report = verify.run_verification(self.db, self.data, "1", "3")
        item = self.item_by_seq(report, 2)
        self.assertEqual(item["verdict"], verify.VERDICT_MISMATCH)
        self.assertEqual(item["occurrences"], 2)
        self.assertEqual(item["log_files"], [ACTIVE])
        self.assertIn(rec(2, 99.9), item["log_raw"])
        self.assertIn("不能判为一致", item["detail"])
        self.assertEqual(report["summary"]["mismatch"], 1)

    def test_same_seq_same_content_twice_is_match(self):
        self.seed_collected([1])
        self.write(ACTIVE, rec(1, 26.0) + "\n")  # 与库内完全相同的重复行
        report = verify.run_verification(self.db, self.data, "1", "1")
        item = report["items"][0]
        self.assertEqual(item["verdict"], verify.VERDICT_MATCH)
        self.assertEqual(item["occurrences"], 2)

    def test_mismatch_in_rotated_file_and_new_active(self):
        self.seed_collected([1, 2])
        self.write(ACTIVE, rec(3) + "\n" + rec(4) + "\n")
        self.assertEqual(collector.collect_once(self.data, self.db)["status"], "ok")
        # 轮转后旧文件继续保持一致；新文件里 seq=3 被篡改
        os.rename(self.active, self.rotated)
        open(self.active, "a").close()
        self.write(ACTIVE, rec(3, 88.8) + "\n")

        report = verify.run_verification(self.db, self.data, "1", "4")
        self.assertEqual(self.item_by_seq(report, 3)["verdict"], verify.VERDICT_MISMATCH)
        self.assertEqual(self.item_by_seq(report, 1)["verdict"], verify.VERDICT_MATCH)
        files = {f["name"]: f for f in report["snapshot"]["files"]}
        self.assertTrue(files[ROTATED]["present"])
        self.assertTrue(files[ACTIVE]["present"])

    def test_missing_when_no_complete_line(self):
        self.seed_collected([1, 2, 3])
        # 新文件里放 seq=2 的半行（无换行），且与库内原文前缀相同
        with open(self.active, "w", encoding="utf-8") as fh:
            fh.write(rec(1) + "\n")
            fh.write(rec(2)[:20])
        report = verify.run_verification(self.db, self.data, "1", "3")
        item = self.item_by_seq(report, 2)
        self.assertEqual(item["verdict"], verify.VERDICT_MISSING)
        self.assertIn("半行不参与核验", item["detail"])
        self.assertEqual(item["occurrences"], 0)
        self.assertEqual(self.item_by_seq(report, 3)["verdict"], verify.VERDICT_MISSING)

    def test_half_line_does_not_upgrade_missing_to_match_even_with_full_prefix(self):
        self.seed_collected([1])
        # 整个库内原文被写成半行（只差换行），仍然是“没有对应完整行”
        with open(self.active, "w", encoding="utf-8") as fh:
            fh.write(rec(1))
        report = verify.run_verification(self.db, self.data, "1", "1")
        self.assertEqual(report["items"][0]["verdict"], verify.VERDICT_MISSING)
        self.assertEqual(report["snapshot"]["files"][1]["half_line_bytes"], len(rec(1)))

    def test_db_gap_is_reported_separately(self):
        self.seed_collected([1, 2, 3, 4, 5])
        # 模拟库内缺失（例如历史清理）：删掉 2、5，范围 1–5 仍有 3 条可核验
        with collector.get_db(self.db) as conn:
            conn.execute("DELETE FROM records WHERE seq IN (2, 5)")
        report = verify.run_verification(self.db, self.data, "1", "5")
        self.assertEqual(report["db_gap_count"], 2)
        self.assertEqual(report["db_gap_sample"], [2, 5])
        self.assertEqual(report["summary"]["total"], 3)
        self.assertEqual([x["seq"] for x in report["items"]], [1, 3, 4])

    def test_verification_does_not_change_collection_progress(self):
        self.seed_collected([1, 2])
        states_before = [tuple(r) for r in collector.list_file_states(self.db)]
        runs_before = len(collector.list_runs(self.db))

        verify.run_verification(self.db, self.data, "1", "2")

        self.assertEqual([tuple(r) for r in collector.list_file_states(self.db)], states_before)
        self.assertEqual(len(collector.list_runs(self.db)), runs_before)
        # 采集仍可继续，且不因核验重复/漏记
        self.write(ACTIVE, rec(3) + "\n")
        r = collector.collect_once(self.data, self.db)
        self.assertEqual(r["records_inserted"], 1)

    def test_range_limits_and_empty_range(self):
        self.seed_collected(list(range(1, 302)))  # 301 条
        with self.assertRaises(verify.VerificationError) as ctx:
            verify.run_verification(self.db, self.data, "1", "301")
        self.assertIn("最多核验 300", str(ctx.exception))
        self.assertEqual(self.reports(), [])  # 失败不发布

        report = verify.run_verification(self.db, self.data, "1", "300")
        self.assertEqual(report["record_count"], 300)

        with self.assertRaises(verify.VerificationError) as ctx:
            verify.run_verification(self.db, self.data, "999", "1000")
        self.assertIn("没有已入库记录", str(ctx.exception))

        with self.assertRaises(verify.VerificationError):
            verify.run_verification(self.db, self.data, "9", "2")
        with self.assertRaises(verify.VerificationError):
            verify.run_verification(self.db, self.data, "abc", "2")
        self.assertEqual(len(self.reports()), 1)

    def test_illegal_log_line_with_seq_counts_as_mismatch(self):
        self.seed_collected([1, 2])
        # 完整但非法的行仍能识别出 seq=2 -> 同号异文（不是“没有对应完整行”）
        self.write(ACTIVE, '{"seq": 2, "ts": "2026-11-15T08:00:00Z", "temp": 2,}\n')
        report = verify.run_verification(self.db, self.data, "1", "2")
        item = self.item_by_seq(report, 2)
        self.assertEqual(item["verdict"], verify.VERDICT_MISMATCH)
        self.assertIn("seq", item["log_raw"])

    def test_unparsed_complete_lines_are_reported_in_snapshot(self):
        self.seed_collected([1])
        self.write(ACTIVE, "garbage without a sequence\n")
        report = verify.run_verification(self.db, self.data, "1", "1")
        files = {f["name"]: f for f in report["snapshot"]["files"]}
        self.assertEqual(files[ACTIVE]["unparsed_lines"], 1)
        self.assertEqual(report["items"][0]["verdict"], verify.VERDICT_MATCH)

    def test_concurrent_collect_does_not_break_reports(self):
        """采集与核验并发：报告要么完整发布，要么不发布；采集进度可继续。"""
        import threading
        import time

        self.seed_collected([1, 2, 3])

        def writer():
            for seq in range(4, 31):
                self.write(ACTIVE, rec(seq) + "\n")
                self.assertIn(
                    collector.collect_once(self.data, self.db)["status"], ("ok", "rejected")
                )
                time.sleep(0.005)

        thread = threading.Thread(target=writer)
        thread.start()
        published = 0
        try:
            while thread.is_alive():
                try:
                    report = verify.run_verification(self.db, self.data, "1", "3")
                except verify.VerificationError:
                    continue
                full = verify.get_report(self.db, report["id"])
                self.assertEqual(len(full["items"]), 3)
                self.assertEqual(
                    report["summary"]["match"]
                    + report["summary"]["mismatch"]
                    + report["summary"]["missing"],
                    3,
                )
                published += 1
        finally:
            thread.join(timeout=10)
        self.assertGreater(published, 0)
        # 所有报告条目数与汇总一致；采集记录完整且连续
        for head in verify.list_reports(self.db):
            self.assertEqual(len(verify.get_report(self.db, head["id"])["items"]), 3)
        seqs, total = self.seqs()
        self.assertEqual(total, 30)
        self.assertEqual(seqs, list(range(1, 31)))

    def test_existing_report_can_be_reread_and_is_immutable(self):
        self.seed_collected([1, 2])
        report = verify.run_verification(self.db, self.data, "1", "2")
        again = verify.get_report(self.db, report["id"])
        self.assertEqual(again, report)
        self.assertEqual(len(verify.list_reports(self.db)), 1)
        self.assertIsNone(verify.get_report(self.db, 12345))

    def test_status_stale_after_new_collection_but_report_kept(self):
        self.seed_collected([1, 2])
        report = verify.run_verification(self.db, self.data, "1", "2")
        status = verify.report_status(self.db, self.data, report)
        self.assertTrue(status["fresh"])

        self.write(ACTIVE, rec(3) + "\n")
        self.assertEqual(collector.collect_once(self.data, self.db)["status"], "ok")

        status = verify.report_status(self.db, self.data, report)
        self.assertFalse(status["fresh"])
        self.assertEqual(status["state"], "stale")
        self.assertTrue(any("库内记录已变化" in r for r in status["reasons"]))
        # 旧报告仍可重读且内容不变
        self.assertEqual(verify.get_report(self.db, report["id"]), report)
        self.assertEqual(len(verify.list_reports(self.db)), 1)

    def test_status_stale_after_log_changes(self):
        self.seed_collected([1, 2])
        report = verify.run_verification(self.db, self.data, "1", "2")

        # 设备续写：大小与 mtime 变化 -> 过期
        self.write(ACTIVE, rec(2) + "\n")
        status = verify.report_status(self.db, self.data, report)
        self.assertFalse(status["fresh"])
        self.assertTrue(any("大小已变化" in r for r in status["reasons"]))

        # 轮转：inode 变化 -> 过期
        os.rename(self.active, self.rotated)
        open(self.active, "a").close()
        status = verify.report_status(self.db, self.data, report)
        self.assertFalse(status["fresh"])
        self.assertTrue(any("轮转/替换" in r or "与报告截面不一致" in r for r in status["reasons"]))

    def test_snapshot_retries_when_file_changes_during_read(self):
        self.seed_collected([1])
        real = verify._capture_file
        calls = {"n": 0}

        def flaky(data_dir, name):
            calls["n"] += 1
            if calls["n"] == 1:
                raise verify._UnstableSnapshot("模拟轮转")
            return real(data_dir, name)

        with mock.patch.object(verify, "_capture_file", side_effect=flaky):
            snaps = verify.capture_snapshot(self.data, retries=3, retry_delay=0)
        self.assertEqual([s.name for s in snaps], [ROTATED, ACTIVE])
        self.assertGreaterEqual(calls["n"], 2)

    def test_snapshot_failure_publishes_nothing_and_keeps_progress(self):
        self.seed_collected([1, 2])
        states_before = [tuple(r) for r in collector.list_file_states(self.db)]
        records_before = collector.list_records(self.db)[1]

        with mock.patch.object(
            verify,
            "capture_snapshot",
            side_effect=verify.SnapshotUnavailable("无法取得自洽截面"),
        ):
            with self.assertRaises(verify.VerificationError):
                verify.run_verification(self.db, self.data, "1", "2")

        self.assertEqual(self.reports(), [])
        self.assertEqual(collector.list_records(self.db)[1], records_before)
        self.assertEqual([tuple(r) for r in collector.list_file_states(self.db)], states_before)

    def test_db_change_during_snapshot_retries_then_succeeds(self):
        self.seed_collected([1, 2])
        real_snapshot = verify.capture_snapshot
        calls = {"n": 0}

        def snapshot_and_advance(data_dir):
            calls["n"] += 1
            snaps = real_snapshot(data_dir)
            if calls["n"] == 1:
                # 模拟“取截面期间采集又推进了库”（范围外新增 seq=3）
                with open(self.active, "a", encoding="utf-8") as fh:
                    fh.write(rec(3) + "\n")
                self.assertEqual(collector.collect_once(self.data, self.db)["status"], "ok")
            return snaps

        with mock.patch.object(verify, "capture_snapshot", side_effect=snapshot_and_advance):
            report = verify.run_verification(self.db, self.data, "1", "2")
        self.assertGreaterEqual(calls["n"], 2)
        self.assertEqual(report["record_count"], 2)
        self.assertEqual(report["summary"]["match"], 2)
        # 水位固定为第二次稳定窗口的值，不是第一次的旧水位
        self.assertEqual(report["watermark"], {"seq": 3, "record_id": 3, "count": 3})

    def test_db_change_that_never_settles_publishes_nothing(self):
        self.seed_collected([1])
        real_snapshot = verify.capture_snapshot
        counter = {"n": 0}

        def snapshot_and_rewrite(data_dir):
            counter["n"] += 1
            snaps = real_snapshot(data_dir)
            with collector.get_db(self.db) as conn:
                seq = 200 + counter["n"]
                conn.execute(
                    "INSERT INTO records(seq, ts, temp, raw, source_path, source_inode, collected_at)"
                    " VALUES (?,?,?,?,?,?,?)",
                    (seq, "2026-11-15T08:00:00Z", 25.0, rec(seq), ACTIVE, 1, "x"),
                )
            return snaps

        with mock.patch.object(verify, "capture_snapshot", side_effect=snapshot_and_rewrite):
            with self.assertRaises(verify.VerificationError) as ctx:
                verify.run_verification(self.db, self.data, "1", "1")
        self.assertIn("本次未发布报告", str(ctx.exception))
        self.assertEqual(self.reports(), [])


class VerificationWebTest(VerifyTestBase):
    def setUp(self):
        super().setUp()
        self.tmp2 = tempfile.TemporaryDirectory()
        self.data = os.path.join(self.tmp2.name, "data")
        self.db = os.path.join(self.tmp2.name, "db", "web.db")
        os.makedirs(self.data, exist_ok=True)
        self.active = os.path.join(self.data, ACTIVE)
        self.rotated = os.path.join(self.data, ROTATED)
        os.environ["DATA_DIR"] = self.data
        os.environ["DB_PATH"] = self.db
        os.environ["ENABLE_DEMO_RESET"] = "1"
        for mod in [m for m in list(sys.modules) if m == "app" or m.startswith("app.")]:
            del sys.modules[mod]
        import app as flask_app  # noqa: WPS433

        self.client = flask_app.create_app().test_client()

    def tearDown(self):
        self.tmp2.cleanup()
        self.tmp.cleanup()

    def test_web_verify_flow_detail_index_and_stale(self):
        self.write(ACTIVE, "".join(rec(i) + "\n" for i in (1, 2, 3)))
        self.client.post("/collect")

        resp = self.client.post(
            "/verify", data={"seq_from": "1", "seq_to": "3"}, follow_redirects=True
        )
        self.assertEqual(resp.status_code, 200)
        text = resp.get_data(as_text=True)
        self.assertIn("核验报告 #1", text)
        self.assertIn("当前有效", text)
        self.assertIn("本次入库水位", text)
        self.assertIn("两份日志的文件截面", text)
        self.assertIn("原文一致", text)

        index = self.client.get("/").get_data(as_text=True)
        self.assertIn("重读报告", index)
        self.assertIn("当前有效", index)

        # 报告可重读
        again = self.client.get("/verify/1")
        self.assertEqual(again.status_code, 200)
        self.assertIn("核验报告 #1", again.get_data(as_text=True))

        # 新增记录 -> 首页显示过期，报告详情仍可访问
        self.write(ACTIVE, rec(4) + "\n")
        self.client.post("/collect")
        index = self.client.get("/").get_data(as_text=True)
        self.assertIn("已过期", index)
        detail = self.client.get("/verify/1").get_data(as_text=True)
        self.assertIn("已过期", detail)
        self.assertIn("报告内容仍完整保留", detail)

    def test_web_mismatch_and_missing_are_marked(self):
        self.write(ACTIVE, "".join(rec(i) + "\n" for i in (1, 2)))
        self.client.post("/collect")
        # seq=1 异文；seq=2 只剩半行
        with open(self.active, "w", encoding="utf-8") as fh:
            fh.write(rec(1, 77.7) + "\n" + rec(2)[:15])
        resp = self.client.post(
            "/verify", data={"seq_from": "1", "seq_to": "2"}, follow_redirects=True
        )
        text = resp.get_data(as_text=True)
        self.assertIn("同号异文", text)
        self.assertIn("没有对应完整行，无法核验", text)
        self.assertIn("不能判为一致", text)

    def test_web_rejects_invalid_input_without_report(self):
        self.write(ACTIVE, rec(1) + "\n")
        self.client.post("/collect")
        cases = [
            {"seq_from": "", "seq_to": "1"},
            {"seq_from": "x", "seq_to": "1"},
            {"seq_from": "5", "seq_to": "1"},
            {"seq_from": "9", "seq_to": "10"},
        ]
        for data in cases:
            resp = self.client.post("/verify", data=data, follow_redirects=True)
            self.assertIn("核验未发布", resp.get_data(as_text=True))
        self.assertEqual(verify.list_reports(self.db), [])
        self.assertNotIn("核验报告 #", self.client.get("/").get_data(as_text=True))

    def test_web_unknown_report_404(self):
        self.assertEqual(self.client.get("/verify/999").status_code, 404)

    def test_web_verify_while_collect_keeps_progress(self):
        self.write(ACTIVE, "".join(rec(i) + "\n" for i in (1, 2)))
        self.client.post("/collect")
        before = [tuple(r) for r in collector.list_file_states(self.db)]
        self.client.post("/verify", data={"seq_from": "1", "seq_to": "2"})
        self.assertEqual([tuple(r) for r in collector.list_file_states(self.db)], before)
        self.write(ACTIVE, rec(3) + "\n")
        resp = self.client.post("/collect", follow_redirects=True)
        self.assertIn("采集成功", resp.get_data(as_text=True))
        self.assertEqual(collector.list_records(self.db)[1], 3)


if __name__ == "__main__":
    unittest.main(verbosity=2)
