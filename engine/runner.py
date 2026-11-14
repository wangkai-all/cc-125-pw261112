"""编排器：按方案矩阵写 Parquet、核对、跑查询、产出 manifest。"""
from __future__ import annotations

import json
import time
import uuid
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from .datagen import DatasetSpec, generate, table_fingerprint
from .queries import QueryConfig, run_query
from .schemes import (Scheme, build_write_kwargs, default_plan,
                      write_kwargs_for_invalid)
from .verify import verify_roundtrip


def _sort_table(table: pa.Table, columns: tuple[str, ...]) -> tuple[pa.Table, float]:
    import pyarrow.compute as pc
    start = time.perf_counter()
    order = pc.sort_indices(
        table, sort_keys=[(c, "ascending") for c in columns])
    out = table.take(order)
    return out, (time.perf_counter() - start) * 1000.0


def _column_metadata(path: str) -> dict:
    pf = pq.ParquetFile(path)
    md = pf.metadata
    arrow_schema = pf.schema_arrow
    rg_count = md.num_row_groups
    cols: dict[str, dict] = {}
    for ci in range(md.num_columns):
        name = md.schema.column(ci).name
        encodings, compressions = set(), set()
        dict_pages, total_comp, total_uncomp = 0, 0, 0
        stats_present = True
        for rg in range(rg_count):
            col = md.row_group(rg).column(ci)
            encodings.update(col.encodings)
            compressions.add(col.compression)
            total_comp += col.total_compressed_size
            total_uncomp += col.total_uncompressed_size
            if col.dictionary_page_offset is not None:
                dict_pages += 1
            if col.statistics is None:
                stats_present = False
        phys = md.schema.column(ci).physical_type
        logical = str(arrow_schema.field(name).type)
        ratio = (total_uncomp / total_comp) if total_comp else 1.0
        cols[name] = {
            "logical_type": logical,
            "physical_type": phys,
            "encodings": sorted(encodings),
            "compression": sorted(compressions)[0],
            "dictionary_pages": dict_pages,
            "has_dictionary": dict_pages > 0,
            "stats_present": stats_present,
            "compressed_bytes": total_comp,
            "uncompressed_bytes": total_uncomp,
            "compression_ratio": round(ratio, 3),
        }
    return {"row_groups": rg_count, "columns": cols,
            "format_version": md.format_version,
            "created_by": md.created_by}


def run_benchmark(
    out_root: str | Path,
    spec: DatasetSpec | None = None,
    schemes: list[Scheme] | None = None,
    query_configs: list[QueryConfig] | None = None,
) -> dict:
    spec = spec or DatasetSpec()
    schemes = schemes or default_plan()
    query_configs = query_configs or [
        QueryConfig("point", "category", repeats=5),
        QueryConfig("range", "amount", repeats=5),
        QueryConfig("scan", "remark", repeats=5),
    ]

    # 时间戳 + 短 uuid，避免同秒启动的两次实验目录互相覆盖
    run_id = (datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
              + "-" + uuid.uuid4().hex[:6])
    run_dir = Path(out_root) / run_id
    data_dir = run_dir / "data"
    data_dir.mkdir(parents=True, exist_ok=True)

    source = generate(spec)
    src_fingerprint = table_fingerprint(source)
    # 源数据也落盘（Arrow IPC，无损），作为可下载输入
    source_path = data_dir / "_source.arrow"
    with pa.ipc.new_stream(source_path.open("wb"), source.schema) as writer:
        writer.write_table(source)
    (data_dir / "_source_preview.json").write_text(
        json.dumps(source.slice(0, 20).to_pylist(), default=str,
                   ensure_ascii=False, indent=2),
        encoding="utf-8")

    results = []
    for scheme in schemes:
        rec: dict = {
            "name": scheme.name, "params": scheme.params_dict(),
            "status": "ok", "failure_reason": None,
        }
        table = source
        sort_ms = 0.0
        if scheme.sort_columns:
            try:
                table, sort_ms = _sort_table(source, scheme.sort_columns)
            except Exception as e:  # pragma: no cover - 防御
                rec.update(status="failed",
                           failure_reason=f"排序失败: {e}")
                results.append(rec)
                continue
        rec["sort_ms"] = round(sort_ms, 3)

        fname = scheme.name.replace("/", "_") + ".parquet"
        path = data_dir / fname
        try:
            if scheme.expect_invalid:
                kwargs = write_kwargs_for_invalid(scheme)
            else:
                kwargs = build_write_kwargs(scheme)
            t0 = time.perf_counter()
            pq.write_table(table, path, **kwargs)
            write_ms = (time.perf_counter() - t0) * 1000.0
        except Exception as e:
            rec.update(status="failed",
                       failure_reason=str(e).splitlines()[0],
                       failure_hint=scheme.invalid_reason_hint,
                       write_kwargs=_safe_kwargs(
                           kwargs if 'kwargs' in locals() else {}))
            if path.exists():
                path.unlink()
            results.append(rec)
            continue

        size = path.stat().st_size
        verify = verify_roundtrip(source, path, expected=table)
        meta = _column_metadata(str(path))

        queries = []
        query_error = None
        for qcfg in query_configs:
            try:
                qr = run_query(str(path), source, qcfg)
                queries.append(qr.to_dict())
            except Exception as e:
                query_error = f"{qcfg.qtype}/{qcfg.field}: {e}"
                break

        rec.update(
            status="ok" if verify.ok and not query_error else "verified_failed",
            failure_reason=(None if verify.ok else "; ".join(verify.errors))
                           or query_error,
            file=fname,
            file_size_bytes=size,
            write_ms=round(write_ms, 3),
            verify=asdict(verify),
            metadata=meta,
            queries=queries,
        )
        results.append(rec)

    manifest = {
        "run_id": run_id,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "engine": {"pyarrow": pa.__version__},
        "dataset": {
            "spec": asdict(spec),
            "fingerprint": spec.fingerprint(),
            "content_fingerprint": src_fingerprint,
            "rows": source.num_rows,
            "columns": [
                {"name": f.name, "type": str(f.type),
                 "null_count": source.column(f.name).null_count}
                for f in source.schema
            ],
            "source_file": "data/_source.arrow",
        },
        "query_configs": [asdict(q) for q in query_configs],
        "schemes": results,
    }
    (run_dir / "manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8")
    return manifest


def _safe_kwargs(kwargs: dict) -> dict:
    out = {}
    for k, v in kwargs.items():
        if isinstance(v, (str, int, float, bool, list, dict)) or v is None:
            out[k] = v
        else:
            out[k] = repr(v)
    return out


if __name__ == "__main__":  # pragma: no cover
    import sys
    root = sys.argv[1] if len(sys.argv) > 1 else "artifacts"
    m = run_benchmark(root)
    ok = sum(1 for s in m["schemes"] if s["status"] == "ok")
    print(f"run {m['run_id']}: {ok}/{len(m['schemes'])} schemes ok")
