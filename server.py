"""标准库 HTTP 服务：静态页面 + 实验 API + 产物下载。

长任务（跑实验）放到后台线程，前端轮询 /api/runs/<id>。
"""
from __future__ import annotations

import json
import threading
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse, parse_qs, quote

from engine.datagen import DatasetSpec
from engine.queries import QueryConfig
from engine.runner import run_benchmark
from engine.schemes import (COMPRESSIONS, PROFILES, Scheme, default_plan,
                            invalid_demo_schemes)

ROOT = Path(__file__).resolve().parent
ARTIFACTS = ROOT / "artifacts"
STATIC = ROOT / "web"

JOBS: dict[str, dict] = {}
JOBS_LOCK = threading.Lock()


def _build_schemes(payload: dict) -> list[Scheme]:
    mode = payload.get("matrix_mode", "default")
    if mode == "default":
        return default_plan(
            compressions=payload.get("compressions") or None,
            profiles=payload.get("profiles") or None,
            row_group_sizes=payload.get("row_group_sizes") or None,
            add_sorted=bool(payload.get("add_sorted", True)))
    # 自定义：前端给完整方案列表
    out = []
    for item in payload.get("custom_schemes", []):
        out.append(Scheme(
            name=item["name"],
            compression=item.get("compression", "snappy"),
            profile=item.get("profile", "defaults"),
            row_group_size=int(item.get("row_group_size", 0) or 0),
            sort_columns=tuple(item.get("sort_columns", ())),
            compression_level=item.get("compression_level"),
        ))
    out.extend(invalid_demo_schemes())
    return out


def _build_queries(payload: dict) -> list[QueryConfig]:
    raw = payload.get("queries") or [
        {"qtype": "point", "field": "category", "repeats": 5},
        {"qtype": "range", "field": "amount", "repeats": 5},
        {"qtype": "scan", "field": "remark", "repeats": 5},
    ]
    out = []
    for q in raw:
        out.append(QueryConfig(
            qtype=q["qtype"], field=q["field"],
            repeats=int(q.get("repeats", 5)),
            lower=q.get("lower"), upper=q.get("upper"),
            needle=q.get("needle"),
            substring=q.get("substring", "warranty")))
    return out


def _validate_payload(payload: dict) -> str | None:
    ds = payload.get("dataset", {})
    rows = int(ds.get("rows", 200_000))
    if not (1 <= rows <= 5_000_000):
        return "行数需在 1 到 5,000,000 之间"
    for key in ("region_null_pct", "remark_null_pct", "event_null_pct"):
        v = float(ds.get(key, 0.1))
        if not (0.0 <= v <= 1.0):
            return f"{key} 必须是 0–1 之间的比例"
    if payload.get("matrix_mode", "default") == "default":
        if not payload.get("compressions"):
            return "至少选择一个压缩算法"
        if not payload.get("profiles"):
            return "至少选择一个编码 profile"
        if not payload.get("row_group_sizes"):
            return "至少选择一个行组大小"
    elif not payload.get("custom_schemes"):
        return "自定义矩阵需要至少一个方案"
    if not payload.get("queries"):
        return "至少配置一条查询"
    for q in payload["queries"]:
        if int(q.get("repeats", 5)) < 1:
            return "查询重复次数必须 >= 1"
    return None


def _run_job(job_id: str, payload: dict):
    try:
        ds = payload.get("dataset", {})
        spec = DatasetSpec(
            rows=int(ds.get("rows", 200_000)),
            seed=int(ds.get("seed", 20261114)),
            region_null_pct=float(ds.get("region_null_pct", 0.08)),
            remark_null_pct=float(ds.get("remark_null_pct", 0.15)),
            event_null_pct=float(ds.get("event_null_pct", 0.05)))
        manifest = run_benchmark(
            ARTIFACTS, spec=spec,
            schemes=_build_schemes(payload),
            query_configs=_build_queries(payload))
        with JOBS_LOCK:
            JOBS[job_id].update(status="done", manifest=manifest,
                                run_id=manifest["run_id"])
    except Exception as e:  # pragma: no cover - 服务层兜底
        import traceback
        with JOBS_LOCK:
            JOBS[job_id].update(status="error", error=str(e),
                                trace=traceback.format_exc())


class Handler(BaseHTTPRequestHandler):
    server_version = "ParquetLab/1.0"

    def log_message(self, fmt, *args):  # 安静日志
        pass

    def _json(self, obj, status=200):
        body = json.dumps(obj, ensure_ascii=False, default=str).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _file(self, path: Path, download_name: str | None = None,
              media: str = "application/octet-stream"):
        if not path.is_file():
            self._json({"error": "not found"}, 404)
            return
        self.send_response(200)
        self.send_header("Content-Type", media)
        self.send_header("Content-Length", str(path.stat().st_size))
        if download_name:
            self.send_header(
                "Content-Disposition",
                f"attachment; filename*=UTF-8''{quote(download_name)}")
        self.end_headers()
        with path.open("rb") as f:
            while chunk := f.read(1 << 16):
                self.wfile.write(chunk)

    def do_GET(self):
        parsed = urlparse(self.path)
        path, qs = parsed.path, parse_qs(parsed.query)

        if path in ("/", "/index.html"):
            return self._file(STATIC / "index.html", media="text/html")
        if path.startswith("/static/"):
            target = (STATIC / path[len("/static/"):]).resolve()
            if not str(target).startswith(str(STATIC.resolve())):
                return self._json({"error": "forbidden"}, 403)
            media = ("text/css" if target.suffix == ".css" else
                     "application/javascript" if target.suffix == ".js"
                     else "text/plain")
            return self._file(target, media=media)

        if path == "/api/options":
            return self._json({
                "compressions": COMPRESSIONS,
                "profiles": PROFILES,
                "point_fields": {
                    "category": "books", "region": "north",
                    "id": None, "ref_code": None},
                "range_fields": ["amount", "score", "quantity", "event_time"],
                "scan_fields": ["remark", "ref_code", "category"],
                "row_group_sizes": [0, 10_000, 50_000, 100_000],
            })

        if path == "/api/runs":
            with JOBS_LOCK:
                jobs = [{"job_id": j["job_id"], "status": j["status"],
                         "run_id": j.get("run_id"),
                         "created_at": j["created_at"]}
                        for j in sorted(JOBS.values(),
                                        key=lambda x: x["created_at"],
                                        reverse=True)]
            return self._json({"jobs": jobs})

        if path.startswith("/api/runs/"):
            job_id = path.split("/")[-1]
            with JOBS_LOCK:
                job = JOBS.get(job_id)
            if not job:
                return self._json({"error": "unknown run"}, 404)
            return self._json({k: v for k, v in job.items()
                               if k not in ("trace",)})

        if path.startswith("/download/"):
            rel = path[len("/download/"):]
            target = (ARTIFACTS / rel).resolve()
            if not str(target).startswith(str(ARTIFACTS.resolve())):
                return self._json({"error": "forbidden"}, 403)
            return self._file(target, download_name=target.name)

        self._json({"error": "not found"}, 404)

    def do_POST(self):
        parsed = urlparse(self.path)
        if parsed.path != "/api/runs":
            return self._json({"error": "not found"}, 404)
        try:
            length = int(self.headers.get("Content-Length", 0))
            payload = json.loads(self.rfile.read(length) or b"{}")
        except json.JSONDecodeError:
            return self._json({"error": "invalid json"}, 400)
        err = _validate_payload(payload)
        if err:
            return self._json({"error": err}, 400)

        job_id = uuid.uuid4().hex[:12]
        from datetime import datetime, timezone
        with JOBS_LOCK:
            JOBS[job_id] = {"job_id": job_id, "status": "running",
                            "created_at": datetime.now(timezone.utc).isoformat(),
                            "request": payload}
        threading.Thread(target=_run_job, args=(job_id, payload),
                         daemon=True).start()
        self._json({"job_id": job_id, "status": "running"}, 202)


def main(host: str = "127.0.0.1", port: int = 8000):
    ARTIFACTS.mkdir(exist_ok=True)
    httpd = ThreadingHTTPServer((host, port), Handler)
    print(f"ParquetLab 运行中: http://{host}:{port}")
    httpd.serve_forever()


if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=8000)
    args = p.parse_args()
    main(args.host, args.port)
