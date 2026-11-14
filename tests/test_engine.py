"""关键语义测试：数据生成确定性、读回核对、失败方案上报、方案参数翻译。

运行：.venv/bin/python -m pytest tests/ -q
（无 pytest 时也可直接 .venv/bin/python tests/test_engine.py）
"""
from __future__ import annotations

import io
from decimal import Decimal

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from engine.datagen import DatasetSpec, generate
from engine.queries import QueryConfig, run_query
from engine.schemes import (Scheme, build_write_kwargs,
                            invalid_demo_schemes, write_kwargs_for_invalid)
from engine.verify import verify_roundtrip


def test_generate_deterministic():
    a = generate(DatasetSpec(rows=3000, seed=123))
    b = generate(DatasetSpec(rows=3000, seed=123))
    assert a.equals(b)
    c = generate(DatasetSpec(rows=3000, seed=124))
    assert not a.equals(c)


def test_schema_shapes_and_nulls():
    t = generate(DatasetSpec(rows=10_000, seed=9))
    assert t.schema.field("amount").type == pa.decimal128(18, 2)
    assert pa.types.is_timestamp(t.schema.field("event_time").type)
    assert t.schema.field("event_time").type.unit == "us"
    for name in ("region", "remark", "event_time"):
        assert t.column(name).null_count > 0
    # 高重复 vs 近唯一
    assert len(set(t.column("category").to_pylist())) <= 6
    assert len(set(t.column("id").to_pylist())) > 9_000


def _write(tmp_path, table, scheme):
    p = tmp_path / f"{scheme.name}.parquet"
    pq.write_table(table, p, **build_write_kwargs(scheme))
    return p


@pytest.mark.parametrize("profile", [
    "defaults", "no_dictionary", "delta",
    "byte_stream_split", "selective_dict", "mixed"])
def test_roundtrip_all_profiles(tmp_path, profile):
    t = generate(DatasetSpec(rows=4000, seed=5))
    p = _write(tmp_path, t, Scheme(name=profile, profile=profile))
    r = verify_roundtrip(t, str(p))
    assert r.ok, r.errors
    assert "Decimal(18,2)" in r.checks["decimal_exactness"]
    assert "timestamp[us]" in r.checks["timestamp_precision"]


def test_decimal_never_float(tmp_path):
    """金额读回必须是 Decimal 且字符串完全一致。"""
    t = generate(DatasetSpec(rows=2000, seed=11))
    p = _write(tmp_path, t, Scheme(name="z", compression="zstd"))
    back = pq.read_table(p)
    for a, b in zip(t.column("amount").to_pylist(),
                    back.column("amount").to_pylist()):
        assert isinstance(b, Decimal)
        assert str(a) == str(b)


def test_sorted_scheme_verifies_after_realignment(tmp_path):
    import pyarrow.compute as pc
    t = generate(DatasetSpec(rows=3000, seed=3))
    order = pc.sort_indices(t, sort_keys=[("category", "ascending")])
    sorted_t = t.take(order)
    p = tmp_path / "sorted.parquet"
    pq.write_table(sorted_t, p, compression="zstd")
    r = verify_roundtrip(t, str(p), expected=sorted_t)
    assert r.ok, r.errors


def test_invalid_schemes_fail_with_reason(tmp_path):
    t = generate(DatasetSpec(rows=1000, seed=1))
    for scheme in invalid_demo_schemes():
        kwargs = write_kwargs_for_invalid(scheme)
        with pytest.raises(Exception) as exc:
            pq.write_table(t, tmp_path / "x.parquet", **kwargs)
        assert str(exc.value)  # 引擎必须给出明确原因


def test_unsupported_compression_rejected_before_write():
    s = Scheme(name="bad", compression="lzo")
    with pytest.raises(ValueError, match="not supported"):
        build_write_kwargs(s)


def test_encoding_profile_kwargs_shape():
    kw = build_write_kwargs(Scheme(name="m", profile="mixed",
                                   compression="gzip", row_group_size=1234))
    assert kw["compression_level"] == 6
    assert kw["row_group_size"] == 1234
    assert "id" in kw["column_encoding"]
    assert "score" in kw["use_byte_stream_split"]
    kw2 = build_write_kwargs(Scheme(name="n", compression="none"))
    assert kw2["compression"] is None


def test_queries_return_repeats_and_median(tmp_path):
    t = generate(DatasetSpec(rows=3000, seed=8))
    p = _write(tmp_path, t, Scheme(name="q"))
    for cfg in [QueryConfig("point", "category", repeats=3),
                QueryConfig("range", "amount", repeats=3),
                QueryConfig("scan", "remark", repeats=3)]:
        r = run_query(str(p), t, cfg)
        assert len(r.timings_ms) == 3
        assert r.median_ms > 0
        assert r.rows_matched >= 0


def test_null_semantics_survive(tmp_path):
    t = generate(DatasetSpec(rows=2500, seed=4))
    p = _write(tmp_path, t, Scheme(name="ns", profile="no_dictionary"))
    back = pq.read_table(p)
    for name in t.column_names:
        assert t.column(name).null_count == back.column(name).null_count


def test_custom_query_parameters(tmp_path):
    """自定义目标值/范围/子串必须被尊重并完成类型转换。"""
    t = generate(DatasetSpec(rows=4000, seed=6))
    p = _write(tmp_path, t, Scheme(name="cq"))
    point = run_query(str(p), t, QueryConfig(
        "point", "id", needle=1234, repeats=2))
    assert point.filters["=="] == 1234
    assert point.rows_matched <= 1
    rng = run_query(str(p), t, QueryConfig(
        "range", "amount", lower="100.00", upper="999.99", repeats=2))
    assert rng.filters[">="] == 100.0 and rng.filters["<="] == 999.99
    scan = run_query(str(p), t, QueryConfig(
        "scan", "remark", substring="refund", repeats=2))
    assert scan.filters["contains"] == "refund"


def test_iso_timestamp_bounds(tmp_path):
    from datetime import datetime
    t = generate(DatasetSpec(rows=4000, seed=6))
    p = _write(tmp_path, t, Scheme(name="ts"))
    r = run_query(str(p), t, QueryConfig(
        "range", "event_time",
        lower="2026-01-10T00:00:00", upper="2026-01-20T00:00:00",
        repeats=2))
    assert isinstance(r.filters[">="], datetime)
    assert r.rows_matched >= 0


def test_payload_validation():
    from server import _validate_payload
    base = {"queries": [{"qtype": "point", "field": "category", "repeats": 3}]}
    assert _validate_payload({**base, "dataset": {"rows": 0}})
    assert _validate_payload({**base, "compressions": []})
    assert _validate_payload({**base, "dataset": {"region_null_pct": 2}})
    assert _validate_payload({"queries": []})
    good = {**base, "compressions": ["zstd"], "profiles": ["delta"],
            "row_group_sizes": [0]}
    assert _validate_payload(good) is None


if __name__ == "__main__":  # 无 pytest 时的轻量入口
    import sys, tempfile
    fails = 0
    with tempfile.TemporaryDirectory() as td:
        from pathlib import Path
        tmp = Path(td)
        for name, fn in list(globals().items()):
            if name.startswith("test_") and callable(fn):
                try:
                    import inspect
                    if "tmp_path" in inspect.signature(fn).parameters:
                        fn(tmp)
                    else:
                        fn()
                    print(f"PASS {name}")
                except Exception as e:
                    fails += 1
                    print(f"FAIL {name}: {e}")
    sys.exit(1 if fails else 0)
