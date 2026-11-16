# -*- coding: utf-8 -*-
"""发酵罐温度日志质控 Web 入口（Flask + SQLite）。"""

from __future__ import annotations

import csv
import io
import os
import threading

from flask import (
    Flask,
    Response,
    abort,
    current_app,
    flash,
    redirect,
    render_template,
    request,
    url_for,
)

from . import collector, verify

DATA_DIR = os.environ.get("DATA_DIR", "/data")
DB_PATH = os.environ.get("DB_PATH", "/instance/fermenter.db")
ENABLE_DEMO_RESET = os.environ.get("ENABLE_DEMO_RESET", "0") == "1"

_collect_lock = threading.Lock()
_verify_lock = threading.Lock()


def create_app() -> Flask:
    app = Flask(__name__)
    app.secret_key = os.environ.get("SECRET_KEY", "fermenter-demo-secret-key")
    os.makedirs(DATA_DIR, exist_ok=True)
    collector.init_db(DB_PATH)
    verify.init_db(DB_PATH)

    @app.get("/")
    def index():
        try:
            page = max(int(request.args.get("page", 1)), 1)
        except (TypeError, ValueError):
            page = 1
        page_size = 25
        rows, total = collector.list_records(
            DB_PATH, limit=page_size, offset=(page - 1) * page_size
        )
        runs = collector.list_runs(DB_PATH)
        states = collector.list_file_states(DB_PATH)
        files = collector._scan_files(DATA_DIR)  # noqa: SLF001 - 展示用
        reports = verify.list_reports(DB_PATH)
        for report in reports:
            report["status"] = verify.report_status(DB_PATH, DATA_DIR, report)
        last_seq = verify.current_watermark(DB_PATH)["seq"]
        return render_template(
            "index.html",
            rows=rows,
            total=total,
            page=page,
            page_size=page_size,
            runs=runs,
            states=states,
            files=files,
            reports=reports,
            last_seq=last_seq,
            max_records=verify.MAX_RECORDS,
            data_dir=DATA_DIR,
            demo_reset=ENABLE_DEMO_RESET,
        )

    @app.post("/collect")
    def collect():
        if not _collect_lock.acquire(blocking=False):
            flash("已有一轮采集正在执行，请稍后刷新查看结果。", "warning")
            return redirect(url_for("index"))
        try:
            result = collector.collect_once(DATA_DIR, DB_PATH)
        finally:
            _collect_lock.release()
        if result["status"] == "ok":
            flash(
                "采集成功：新增 {ins} 条，重复跳过 {dup} 条，完整行 {seen} 行，"
                "半行 {half} 字节留待下次。".format(
                    ins=result["records_inserted"],
                    dup=result["duplicates"],
                    seen=result["lines_seen"],
                    half=result["half_line_bytes"],
                ),
                "success",
            )
        else:
            level = "danger" if result["status"] == "rejected" else "warning"
            flash(
                "整轮{}（已入库记录与采集进度不变）：{}".format(
                    "拒绝" if result["status"] == "rejected" else "出错",
                    result["error"],
                ),
                level,
            )
        return redirect(url_for("index"))

    @app.get("/records.csv")
    def export_csv():
        rows, _ = collector.list_records(DB_PATH, limit=10_000_000, offset=0)
        buf = io.StringIO()
        writer = csv.writer(buf)
        writer.writerow(["seq", "ts_utc", "temp", "source_file", "inode", "collected_at", "raw"])
        for r in rows:
            writer.writerow(
                [r["seq"], r["ts"], r["temp"], r["source_path"], r["source_inode"], r["collected_at"], r["raw"]]
            )
        # utf-8-sig 便于 Excel 直接打开中文不乱码；rows 按 seq 倒序，导出时转正序
        text = buf.getvalue()
        lines = text.splitlines()
        header, body = lines[0], lines[1:]
        body.reverse()
        payload = "﻿" + header + "\r\n" + "\r\n".join(body) + ("\r\n" if body else "")
        return Response(
            payload,
            mimetype="text/csv; charset=utf-8",
            headers={"Content-Disposition": "attachment; filename=temperature_records.csv"},
        )

    @app.post("/demo/reset")
    def demo_reset():
        if not ENABLE_DEMO_RESET:
            flash("演示重置未开启（需 ENABLE_DEMO_RESET=1）。", "warning")
            return redirect(url_for("index"))
        collector.reset_all(DB_PATH, DATA_DIR)
        verify.clear_reports(DB_PATH)
        flash("已清空数据库（含核验报告）与挂载目录中的日志文件。", "success")
        return redirect(url_for("index"))

    @app.post("/verify")
    def verify_records():
        seq_from = request.form.get("seq_from", "").strip()
        seq_to = request.form.get("seq_to", "").strip()
        if not _verify_lock.acquire(blocking=False):
            flash("已有一份核验正在执行，请稍后刷新查看结果。", "warning")
            return redirect(url_for("index") + "#verify")
        try:
            report = verify.run_verification(DB_PATH, DATA_DIR, seq_from, seq_to)
        except verify.VerificationError as exc:
            flash(f"核验未发布：{exc}", "danger")
            return redirect(url_for("index") + "#verify")
        except Exception as exc:  # noqa: BLE001 - 失败也不能留下半份报告
            current_app.logger.exception("核验内部错误")
            flash(f"核验未发布：内部错误（{type(exc).__name__}: {exc}）。", "danger")
            return redirect(url_for("index") + "#verify")
        finally:
            _verify_lock.release()
        flash(
            "核验报告 #{id} 已发布：核验 {n} 条，原文一致 {m}，同号异文 {d}，"
            "无法核验 {x}。".format(
                id=report["id"],
                n=report["record_count"],
                m=report["summary"]["match"],
                d=report["summary"]["mismatch"],
                x=report["summary"]["missing"],
            ),
            "success",
        )
        return redirect(url_for("verify_detail", report_id=report["id"]))

    @app.get("/verify/<int:report_id>")
    def verify_detail(report_id: int):
        report = verify.get_report(DB_PATH, report_id)
        if report is None:
            abort(404)
        status = verify.report_status(DB_PATH, DATA_DIR, report)
        return render_template(
            "verify_report.html",
            report=report,
            status=status,
            data_dir=DATA_DIR,
            max_records=verify.MAX_RECORDS,
        )

    @app.get("/healthz")
    def healthz():
        return {"status": "ok"}

    return app


if __name__ == "__main__":
    create_app().run(host="0.0.0.0", port=int(os.environ.get("PORT", "8000")))
