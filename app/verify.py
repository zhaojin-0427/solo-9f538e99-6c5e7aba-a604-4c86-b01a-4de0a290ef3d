# -*- coding: utf-8 -*-
"""按序号范围核验“库内原文”与“设备仍保留的日志”。

与采集器解耦
============
本模块只读取 ``records``、``file_state`` 与两份日志，**不写**采集相关表，
也不推进采集进度。核验结果写入独立的 ``verify_reports`` / ``verify_items``
两张表，整份报告在一个事务内落库：失败不留半份结果。

核验规则
========
1. 一次最多核验 300 条**已入库**记录（按 seq 升序）。
2. 只比较两份日志中**以换行结束的完整行**；文件末尾没有换行的半行不参与，
   即使它的字节与库内原文前缀相同。
3. 同一 ``seq`` 在任一文件中出现多次时：
   - 所有出现都与库内原文逐字节相同 -> “原文一致”；
   - 只要有一次不同 -> “同号异文”（不能因为另一次相同就判为一致）；
   - 完全没有该序号的完整行 -> “没有对应完整行，无法核验”。

自洽截面
========
设备可能随时续写、轮转，采集也可能同时进行。核验用文件描述符读取两份日志
在某一时刻的前缀，并在读完后复核“路径仍指向同一 inode、文件未被截断”；
任一步不满足就重试，重试耗尽则**不发布报告**。报告固定：

- 本次入库水位（核验时库内记录总数、最大 seq、最大记录 id）；
- 两份日志的文件截面（是否存在、inode、字节数、mtime_ns、完整行数、
  半行字节数、所读前缀的 SHA-256）。

已发布报告可反复读取。之后库内新增记录，或所核日志的大小/inode/mtime 变化，
旧报告仍保留，但当前状态显示为“已过期”。
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
import time
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from . import collector

MAX_RECORDS = 300
SNAPSHOT_RETRIES = 6
SNAPSHOT_RETRY_DELAY = 0.05
GAP_SAMPLE_LIMIT = 20

VERDICT_MATCH = "match"
VERDICT_MISMATCH = "mismatch"
VERDICT_MISSING = "missing"

VERDICT_LABELS = {
    VERDICT_MATCH: "原文一致",
    VERDICT_MISMATCH: "同号异文",
    VERDICT_MISSING: "没有对应完整行，无法核验",
}

_SEQ_RE = re.compile(rb'"seq"\s*:\s*(-?\d+)')


class VerificationError(Exception):
    """可向用户展示的核验失败原因；不会留下任何报告。"""


class SnapshotUnavailable(VerificationError):
    """无法取得自洽的文件截面。"""


# --------------------------------------------------------------------------- #
# 数据库
# --------------------------------------------------------------------------- #
def init_db(db_path: str) -> None:
    """建立核验专用表；可重复调用，不触碰采集表。"""
    with collector.get_db(db_path) as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS verify_reports (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at      TEXT    NOT NULL,
                seq_from        INTEGER NOT NULL,
                seq_to          INTEGER NOT NULL,
                record_count    INTEGER NOT NULL,
                db_gap_count    INTEGER NOT NULL DEFAULT 0,
                db_gap_sample   TEXT    NOT NULL DEFAULT '[]',
                watermark_seq   INTEGER,
                watermark_id    INTEGER,
                watermark_count INTEGER NOT NULL,
                snapshot_json   TEXT    NOT NULL,
                matched         INTEGER NOT NULL,
                mismatched      INTEGER NOT NULL,
                missing         INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS verify_items (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                report_id  INTEGER NOT NULL REFERENCES verify_reports(id) ON DELETE CASCADE,
                seq        INTEGER NOT NULL,
                verdict    TEXT    NOT NULL,
                db_raw     TEXT    NOT NULL,
                log_raw    TEXT,
                log_files  TEXT    NOT NULL DEFAULT '[]',
                occurrences INTEGER NOT NULL DEFAULT 0,
                detail     TEXT    NOT NULL DEFAULT '',
                UNIQUE(report_id, seq)
            );

            CREATE INDEX IF NOT EXISTS idx_verify_items_report
                ON verify_items(report_id, seq);
            """
        )


# --------------------------------------------------------------------------- #
# 文件截面
# --------------------------------------------------------------------------- #
@dataclass
class FileSnapshot:
    name: str
    present: bool
    inode: Optional[int] = None
    size: int = 0
    mtime_ns: Optional[int] = None
    ctime_ns: Optional[int] = None
    sha256: Optional[str] = None
    complete_bytes: int = 0
    complete_lines: List[bytes] = field(default_factory=list)
    half_line_bytes: int = 0
    half_line: bytes = b""
    unparsed_lines: int = 0

    def public(self) -> dict:
        return {
            "name": self.name,
            "present": self.present,
            "inode": self.inode,
            "size": self.size,
            "mtime_ns": self.mtime_ns,
            "ctime_ns": self.ctime_ns,
            "sha256": self.sha256,
            "complete_lines": len(self.complete_lines),
            "complete_bytes": self.complete_bytes,
            "half_line_bytes": self.half_line_bytes,
            "unparsed_lines": self.unparsed_lines,
        }


class _UnstableSnapshot(Exception):
    """读取过程中文件发生变化，需要重试。"""


def _split_complete_lines(data: bytes) -> Tuple[List[bytes], int, bytes]:
    """按最后一个换行切出完整行；末尾无换行的半行原样返回，不参与核验。"""
    last_nl = data.rfind(b"\n")
    if last_nl < 0:
        return [], 0, data
    complete = data[: last_nl + 1]
    tail = data[last_nl + 1 :]
    lines = complete.split(b"\n")[:-1]
    return lines, len(complete), tail


def _capture_file(data_dir: str, name: str) -> FileSnapshot:
    """读取一个日志文件的截面。

    读取前后对同一个 fd 做 fstat：inode、大小或 mtime 变化即认为读取期间
    设备有写入，交由上层重试；读到的字节数必须与读取前的大小一致。
    """
    path = os.path.join(data_dir, name)
    try:
        fd = os.open(path, os.O_RDONLY)
    except FileNotFoundError:
        return FileSnapshot(name=name, present=False)
    except OSError as exc:
        raise VerificationError(f"无法打开日志 {name}：{exc}") from exc

    try:
        st1 = os.fstat(fd)
        if not stat.S_ISREG(st1.st_mode):
            raise VerificationError(f"日志 {name} 不是普通文件")
        chunks: List[bytes] = []
        while True:
            try:
                chunk = os.read(fd, 1024 * 1024)
            except OSError as exc:
                raise VerificationError(f"读取日志 {name} 失败：{exc}") from exc
            if not chunk:
                break
            chunks.append(chunk)
        st2 = os.fstat(fd)
    finally:
        os.close(fd)

    data = b"".join(chunks)
    if (
        st1.st_ino != st2.st_ino
        or st1.st_size != st2.st_size
        or st1.st_mtime_ns != st2.st_mtime_ns
        or len(data) != st1.st_size
    ):
        raise _UnstableSnapshot(f"{name} 在读取期间大小或修改时间发生变化")

    lines, complete_bytes, tail = _split_complete_lines(data)
    unparsed = sum(1 for line in lines if _extract_seq(line) is None)
    return FileSnapshot(
        name=name,
        present=True,
        inode=st1.st_ino,
        size=st1.st_size,
        mtime_ns=st1.st_mtime_ns,
        ctime_ns=st1.st_ctime_ns,
        sha256=hashlib.sha256(data).hexdigest(),
        complete_bytes=complete_bytes,
        complete_lines=lines,
        half_line_bytes=len(tail),
        half_line=tail,
        unparsed_lines=unparsed,
    )


def _check_path_mapping(data_dir: str, snaps: Sequence[FileSnapshot]) -> None:
    """读完后再确认路径仍指向同一 inode，且文件没有被截断。

    只允许“读取后继续追加”（大小变大）：此前读到的前缀仍是一份有效截面。
    """
    for snap in snaps:
        path = os.path.join(data_dir, snap.name)
        try:
            st = os.stat(path)
        except FileNotFoundError:
            if snap.present:
                raise _UnstableSnapshot(f"{snap.name} 在读取后消失（可能正在轮转）")
            continue
        except OSError as exc:
            raise _UnstableSnapshot(f"{snap.name} 读取后无法复核：{exc}")

        if not snap.present:
            raise _UnstableSnapshot(f"{snap.name} 在读取后出现（可能正在轮转）")
        if st.st_ino != snap.inode:
            raise _UnstableSnapshot(f"{snap.name} 在读取期间被替换或轮转")
        if st.st_size < snap.size:
            raise _UnstableSnapshot(f"{snap.name} 在读取期间被截断")


def capture_snapshot(
    data_dir: str,
    retries: int = SNAPSHOT_RETRIES,
    retry_delay: float = SNAPSHOT_RETRY_DELAY,
) -> List[FileSnapshot]:
    """取得两份日志的自洽截面；失败抛 SnapshotUnavailable（不留下任何结果）。"""
    last_reason = "未知原因"
    for attempt in range(retries):
        try:
            snaps = [_capture_file(data_dir, name) for name in collector.LOG_FILES]
            _check_path_mapping(data_dir, snaps)
            if (
                len(snaps) == 2
                and snaps[0].present
                and snaps[1].present
                and snaps[0].inode == snaps[1].inode
            ):
                raise _UnstableSnapshot("两份日志指向同一 inode")
            return snaps
        except _UnstableSnapshot as exc:
            last_reason = str(exc)
            if attempt < retries - 1 and retry_delay > 0:
                time.sleep(retry_delay)
    raise SnapshotUnavailable(
        f"无法取得自洽的文件截面（已重试 {retries} 次）：{last_reason}。"
        "设备可能正在轮转/写入，本次未发布报告。"
    )


def current_file_signature(data_dir: str) -> List[dict]:
    """用于判断“所核日志是否变化”的轻量签名（不读文件内容）。"""
    result = []
    for name in collector.LOG_FILES:
        path = os.path.join(data_dir, name)
        try:
            st = os.stat(path)
        except FileNotFoundError:
            result.append({"name": name, "present": False})
            continue
        except OSError:
            result.append({"name": name, "present": False, "unreadable": True})
            continue
        result.append(
            {
                "name": name,
                "present": True,
                "inode": st.st_ino,
                "size": st.st_size,
                "mtime_ns": st.st_mtime_ns,
            }
        )
    return result


# --------------------------------------------------------------------------- #
# 库内快照与匹配
# --------------------------------------------------------------------------- #
def _read_db_snapshot(db_path: str, seq_from: int, seq_to: int) -> dict:
    """在一个只读事务中读取入库水位与范围内记录（采集可继续）。

    只取 ``MAX_RECORDS + 1`` 条：足以判断是否超限，又不会把超大范围整表载入。
    """
    conn = collector.get_db(db_path)
    try:
        conn.execute("BEGIN")
        try:
            watermark = conn.execute(
                "SELECT COUNT(*) AS count, MAX(seq) AS max_seq, MAX(id) AS max_id FROM records"
            ).fetchone()
            rows = conn.execute(
                "SELECT seq, raw FROM records WHERE seq >= ? AND seq <= ?"
                " ORDER BY seq LIMIT ?",
                (seq_from, seq_to, MAX_RECORDS + 1),
            ).fetchall()
            conn.execute("COMMIT")
        except Exception:
            conn.rollback()
            raise
    finally:
        conn.close()
    return {
        "watermark": {
            "count": watermark["count"],
            "seq": watermark["max_seq"],
            "record_id": watermark["max_id"],
        },
        "rows": rows,
    }


def current_watermark(db_path: str) -> dict:
    with collector.get_db(db_path) as conn:
        row = conn.execute(
            "SELECT COUNT(*) AS count, MAX(seq) AS max_seq, MAX(id) AS max_id FROM records"
        ).fetchone()
    return {"count": row["count"], "seq": row["max_seq"], "record_id": row["max_id"]}


def _same_watermark(a: dict, b: dict) -> bool:
    return (
        a["count"] == b["count"]
        and a["seq"] == b["seq"]
        and a["record_id"] == b["record_id"]
    )


def _extract_seq(line: bytes) -> Optional[int]:
    """从完整行中提取 seq；合法 JSON 优先，失败时做宽松匹配以识别同号异文。"""
    try:
        text = line.decode("utf-8")
    except UnicodeDecodeError:
        text = None
    if text is not None:
        try:
            obj = json.loads(text)
            if isinstance(obj, dict):
                seq = obj.get("seq")
                if isinstance(seq, int) and not isinstance(seq, bool):
                    return seq
        except json.JSONDecodeError:
            pass
    match = _SEQ_RE.search(line)
    if match:
        try:
            return int(match.group(1))
        except ValueError:
            return None
    return None


def _gap_sample(seq_from: int, seq_to: int, present: Iterable[int], limit: int) -> List[int]:
    """返回范围内库内缺失序号的前若干个（不遍历超大区间）。"""
    missing: List[int] = []
    expected = seq_from
    for seq in sorted(present):
        if seq < expected:
            continue
        if seq > expected:
            take = min(limit - len(missing), seq - expected)
            missing.extend(range(expected, expected + take))
            if len(missing) >= limit:
                return missing
        expected = seq + 1
    if expected <= seq_to:
        take = min(limit - len(missing), seq_to - expected + 1)
        missing.extend(range(expected, expected + take))
    return missing


def _build_verdicts(
    rows: Sequence,
    snaps: Sequence[FileSnapshot],
) -> Tuple[List[dict], dict]:
    """把两份日志的完整行按 seq 归并，逐条给出核验结论。"""
    occurrences: Dict[int, List[Tuple[str, bytes]]] = {}
    half_seqs: Dict[int, List[str]] = {}

    for snap in snaps:
        if not snap.present:
            continue
        for line in snap.complete_lines:
            seq = _extract_seq(line)
            if seq is not None:
                occurrences.setdefault(seq, []).append((snap.name, line))
        half_seq = _extract_seq(snap.half_line) if snap.half_line else None
        if half_seq is not None:
            half_seqs.setdefault(half_seq, []).append(snap.name)

    items: List[dict] = []
    counts = {VERDICT_MATCH: 0, VERDICT_MISMATCH: 0, VERDICT_MISSING: 0}

    for row in rows:
        seq = row["seq"]
        db_raw = row["raw"]
        db_bytes = db_raw.encode("utf-8")
        occ = occurrences.get(seq, [])
        half_files = half_seqs.get(seq, [])

        if not occ:
            verdict = VERDICT_MISSING
            log_raw = None
            log_files: List[str] = []
            detail = "两份日志的完整行中未找到该序号。"
            if half_files:
                detail += "该序号只出现在半行中（半行不参与核验）。"
        else:
            log_files = [name for name, _ in occ]
            differing = [(name, line) for name, line in occ if line != db_bytes]
            if differing:
                verdict = VERDICT_MISMATCH
                first_line = differing[0][1]
                try:
                    log_raw = first_line.decode("utf-8")
                except UnicodeDecodeError:
                    log_raw = first_line.decode("utf-8", "backslashreplace")
                detail = (
                    f"该序号在日志完整行中出现 {len(occ)} 次，"
                    f"其中 {len(differing)} 次与库内原文不同；"
                    "同号出现异文不能判为一致。"
                )
                if half_files:
                    detail += "另有半行出现该序号（半行不参与核验）。"
            else:
                verdict = VERDICT_MATCH
                log_raw = db_raw
                detail = f"该序号在日志完整行中出现 {len(occ)} 次，内容均与库内原文一致。"
                if half_files:
                    detail += "另有半行出现该序号（半行不参与核验）。"

        counts[verdict] += 1
        items.append(
            {
                "seq": seq,
                "verdict": verdict,
                "label": VERDICT_LABELS[verdict],
                "db_raw": db_raw,
                "log_raw": log_raw,
                "log_files": sorted(set(log_files)),
                "occurrences": len(occ),
                "detail": detail,
            }
        )

    return items, counts


# --------------------------------------------------------------------------- #
# 生成与读取报告
# --------------------------------------------------------------------------- #
def _as_int(value, field_name: str) -> int:
    if isinstance(value, bool):
        raise VerificationError(f"{field_name}必须是整数。")
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        text = value.strip()
        if text:
            try:
                return int(text, 10)
            except ValueError:
                pass
    raise VerificationError(f"{field_name}必须是整数。")


def run_verification(db_path: str, data_dir: str, seq_from, seq_to) -> dict:
    """执行一次核验并发布报告。

    先读库内水位，再取日志截面，最后整份报告单事务写入。任何失败都会抛出
    VerificationError；不会留下报告，也不会改动采集记录/进度。
    """
    start = _as_int(seq_from, "起始序号")
    end = _as_int(seq_to, "结束序号")
    if start > end:
        raise VerificationError("起始序号不能大于结束序号。")

    init_db(db_path)
    # 自洽性：报告的水位必须与日志截面属于同一时刻的库状态。
    # 先读库水位与范围内记录，再取日志截面，最后复读库水位；只有两次水位
    # （总数、最大 seq、最大记录 id）完全一致才发布。采集可以在核验期间继续，
    # 但若它推进了库，本次截面就无法固定到那个水位，重取直至稳定；
    # 有限次仍不稳定则不发布报告。
    for attempt in range(1, 4):
        db_snapshot = _read_db_snapshot(db_path, start, end)
        rows = db_snapshot["rows"]
        if not rows:
            raise VerificationError(
                f"序号范围 {start}–{end} 内没有已入库记录，无法核验。"
            )
        if len(rows) > MAX_RECORDS:
            raise VerificationError(
                f"一次最多核验 {MAX_RECORDS} 条已入库记录，"
                f"当前范围已超过 {MAX_RECORDS} 条；请缩小序号范围。"
            )

        snaps = capture_snapshot(data_dir)
        db_after = _read_db_snapshot(db_path, start, end)
        # 两次水位（总数、最大 seq、最大记录 id）完全一致，才能说“这份日志
        # 截面对应报告里固定的本次入库水位”。采集在核验窗口内推进了就重取；
        # 有限次仍不稳定则不发布，避免发布一份自相矛盾或出生即过期的报告。
        if _same_watermark(db_after["watermark"], db_snapshot["watermark"]):
            break
        if attempt == 3:
            raise VerificationError(
                "核验期间入库水位持续变化，无法把日志截面固定到本次入库水位，"
                "本次未发布报告；请稍后重试。"
            )

    items, counts = _build_verdicts(rows, snaps)

    present = {row["seq"] for row in rows}
    gap_count = (end - start + 1) - len(present)
    gap_sample = _gap_sample(start, end, present, GAP_SAMPLE_LIMIT)

    created_at = collector.utc_now_iso()
    snapshot_public = {
        "captured_at": created_at,
        "files": [snap.public() for snap in snaps],
    }
    watermark = db_snapshot["watermark"]
    header = {
        "created_at": created_at,
        "seq_from": start,
        "seq_to": end,
        "record_count": len(rows),
        "db_gap_count": gap_count,
        "db_gap_sample": gap_sample,
        "watermark_seq": watermark["seq"],
        "watermark_id": watermark["record_id"],
        "watermark_count": watermark["count"],
        "snapshot_json": json.dumps(snapshot_public, ensure_ascii=False),
        "matched": counts[VERDICT_MATCH],
        "mismatched": counts[VERDICT_MISMATCH],
        "missing": counts[VERDICT_MISSING],
    }

    conn = collector.get_db(db_path)
    try:
        with conn:  # 单事务：报告 + 明细，失败整体回滚
            cur = conn.execute(                "INSERT INTO verify_reports(created_at, seq_from, seq_to, record_count,"
                " db_gap_count, db_gap_sample, watermark_seq, watermark_id, watermark_count,"
                " snapshot_json, matched, mismatched, missing)"
                " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    header["created_at"],
                    header["seq_from"],
                    header["seq_to"],
                    header["record_count"],
                    header["db_gap_count"],
                    json.dumps(header["db_gap_sample"]),
                    header["watermark_seq"],
                    header["watermark_id"],
                    header["watermark_count"],
                    header["snapshot_json"],
                    header["matched"],
                    header["mismatched"],
                    header["missing"],
                ),
            )
            report_id = cur.lastrowid
            conn.executemany(
                "INSERT INTO verify_items(report_id, seq, verdict, db_raw, log_raw,"
                " log_files, occurrences, detail) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                [
                    (
                        report_id,
                        item["seq"],
                        item["verdict"],
                        item["db_raw"],
                        item["log_raw"],
                        json.dumps(item["log_files"], ensure_ascii=False),
                        item["occurrences"],
                        item["detail"],
                    )
                    for item in items
                ],
            )
    finally:
        conn.close()

    report = get_report(db_path, report_id)
    if report is None:  # 理论上不可能
        raise VerificationError("报告写入后无法读取，请重试。")
    return report


def _row_to_report(row, items: Optional[List] = None) -> dict:
    snapshot = json.loads(row["snapshot_json"])
    report = {
        "id": row["id"],
        "created_at": row["created_at"],
        "range": {"from": row["seq_from"], "to": row["seq_to"]},
        "record_count": row["record_count"],
        "db_gap_count": row["db_gap_count"],
        "db_gap_sample": json.loads(row["db_gap_sample"]),
        "watermark": {
            "seq": row["watermark_seq"],
            "record_id": row["watermark_id"],
            "count": row["watermark_count"],
        },
        "snapshot": snapshot,
        "summary": {
            "total": row["record_count"],
            "match": row["matched"],
            "mismatch": row["mismatched"],
            "missing": row["missing"],
        },
    }
    if items is not None:
        report["items"] = items
    return report


def get_report(db_path: str, report_id: int) -> Optional[dict]:
    init_db(db_path)
    with collector.get_db(db_path) as conn:
        row = conn.execute(
            "SELECT * FROM verify_reports WHERE id = ?", (report_id,)
        ).fetchone()
        if row is None:
            return None
        item_rows = conn.execute(
            "SELECT * FROM verify_items WHERE report_id = ? ORDER BY seq", (report_id,)
        ).fetchall()
    items = [
        {
            "seq": item["seq"],
            "verdict": item["verdict"],
            "label": VERDICT_LABELS.get(item["verdict"], item["verdict"]),
            "db_raw": item["db_raw"],
            "log_raw": item["log_raw"],
            "log_files": json.loads(item["log_files"]),
            "occurrences": item["occurrences"],
            "detail": item["detail"],
        }
        for item in item_rows
    ]
    return _row_to_report(row, items)


def list_reports(db_path: str, limit: int = 100) -> List[dict]:
    init_db(db_path)
    with collector.get_db(db_path) as conn:
        rows = conn.execute(
            "SELECT * FROM verify_reports ORDER BY id DESC LIMIT ?", (limit,)
        ).fetchall()
    return [_row_to_report(row) for row in rows]


def clear_reports(db_path: str) -> None:
    """演示重置用：清空核验报告与明细（不影响采集表）。"""
    init_db(db_path)
    with collector.get_db(db_path) as conn:
        conn.execute("DELETE FROM verify_items")
        conn.execute("DELETE FROM verify_reports")


def report_status(db_path: str, data_dir: str, report: dict) -> dict:
    """计算报告“当前是否仍然有效”，不修改报告本身。"""
    reasons: List[str] = []
    try:
        current_wm = current_watermark(db_path)
    except Exception as exc:  # noqa: BLE001 - 状态展示不能 500
        return {"state": "stale", "fresh": False, "reasons": [f"无法读取库内水位：{exc}"]}

    wm = report["watermark"]
    if (
        current_wm["count"] != wm["count"]
        or current_wm["seq"] != wm["seq"]
        or current_wm["record_id"] != wm["record_id"]
    ):
        reasons.append(
            "库内记录已变化：核验时 {c} 条（最大序号 {s}），"
            "当前 {cc} 条（最大序号 {cs}）。".format(
                c=wm["count"],
                s=wm["seq"] if wm["seq"] is not None else "—",
                cc=current_wm["count"],
                cs=current_wm["seq"] if current_wm["seq"] is not None else "—",
            )
        )

    old_files = {f["name"]: f for f in report["snapshot"]["files"]}
    for cur in current_file_signature(data_dir):
        old = old_files.get(cur["name"])
        if old is None:
            reasons.append(f"报告缺少 {cur['name']} 的截面信息。")
            continue
        if bool(cur.get("present")) != bool(old.get("present")):
            reasons.append(
                f"{cur['name']} 已{'出现' if cur.get('present') else '缺失'}，"
                "与报告截面不一致。"
            )
            continue
        if not cur.get("present"):
            continue
        if cur.get("unreadable"):
            reasons.append(f"{cur['name']} 当前无法读取。")
            continue
        if cur.get("inode") != old.get("inode"):
            reasons.append(
                f"{cur['name']} 已被轮转/替换（inode {old.get('inode')} → {cur.get('inode')}）。"
            )
        elif cur.get("size") != old.get("size"):
            reasons.append(
                f"{cur['name']} 大小已变化（{old.get('size')} → {cur.get('size')} 字节）。"
            )
        elif cur.get("mtime_ns") != old.get("mtime_ns"):
            reasons.append(f"{cur['name']} 修改时间已变化。")

    return {"state": "fresh" if not reasons else "stale", "fresh": not reasons, "reasons": reasons}
