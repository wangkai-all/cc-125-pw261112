"""查询基准：三类查询，每次重复独立打开文件，记录全部原始耗时与中位数。

三类查询覆盖不同 Parquet 读取路径：
  point     等值点查（高选择度，依赖字典/统计裁剪/bloom 类特性）
  range     过滤 + 聚合（金额/分数阈值，行组统计裁剪 + 向量化聚合）
  scan      长文本子串扫描（必须解码字符串页，编码影响明显）

每次重复都通过 pq.read_table 重新打开文件读取，避免页缓存把单次偶然
最快误判成结论。
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from statistics import median
from typing import Any, Callable

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

QUERY_TYPES = ["point", "range", "scan"]

# 可在页面切换的查询字段（每类给出可选列与取值生成方式）
POINT_FIELDS = {
    "category": {"kind": "string", "sample": "books"},
    "region": {"kind": "string", "sample": "north"},
    "id": {"kind": "int", "sample_quantile": 0.5},
    "ref_code": {"kind": "string", "sample_from_data": True},
}
RANGE_FIELDS = ["amount", "score", "quantity", "event_time"]
SCAN_FIELDS = ["remark", "ref_code", "category"]


@dataclass
class QueryConfig:
    qtype: str = "point"
    field: str = "category"
    repeats: int = 5
    # 过滤范围参数（range 类型用）；None 时按数据自动取分位数
    lower: Any = None
    upper: Any = None
    # point 查询的目标值；None 时自动采样
    needle: Any = None
    # scan 子串
    substring: str = "warranty"


@dataclass
class QueryResult:
    qtype: str
    field: str
    repeats: int
    timings_ms: list[float] = field(default_factory=list)
    median_ms: float = 0.0
    rows_matched: int = 0
    detail: str = ""
    filters: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "qtype": self.qtype, "field": self.field, "repeats": self.repeats,
            "timings_ms": [round(t, 3) for t in self.timings_ms],
            "median_ms": round(self.median_ms, 3),
            "rows_matched": self.rows_matched, "detail": self.detail,
            "filters": self.filters,
        }


def _time_once(fn: Callable[[], Any]) -> tuple[float, Any]:
    start = time.perf_counter()
    out = fn()
    return (time.perf_counter() - start) * 1000.0, out


def _scalar_py(value):
    """pc.sum/quantile 可能返回 Scalar 或长度为 1 的数组，统一取值。"""
    if hasattr(value, "as_py"):
        return value.as_py()
    return value[0].as_py()


def _resolve_needle(table: pa.Table, cfg: QueryConfig) -> Any:
    if cfg.needle is not None:
        return _coerce_to_column(cfg.needle, table.column(cfg.field).type)
    meta = POINT_FIELDS.get(cfg.field, {})
    if meta.get("sample_from_data"):
        return table.column(cfg.field)[len(table) // 3].as_py()
    if "sample" in meta:
        return meta["sample"]
    if meta.get("kind") == "int":
        return int(table.column(cfg.field)[len(table) // 2].as_py())
    return table.column(cfg.field)[0].as_py()


def _coerce_to_column(value: Any, target_type: pa.DataType) -> Any:
    """把前端 JSON 传来的字符串/数字转成与列类型匹配的 Python 值。"""
    if pa.types.is_timestamp(target_type) and isinstance(value, str):
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    if pa.types.is_decimal(target_type) and not isinstance(value, Decimal):
        d = Decimal(str(value))
        # JSON 不支持 Decimal，转成保留精度的 float 仅用于过滤值与展示；
        # 数据列本身始终是 Decimal，不参与任何浮点计算
        return float(d)
    if pa.types.is_integer(target_type) and isinstance(value, str):
        return int(value)
    return value


def _resolve_bounds(table: pa.Table, cfg: QueryConfig) -> tuple[Any, Any]:
    col = table.column(cfg.field).combine_chunks()
    if cfg.lower is not None and cfg.upper is not None:
        return (_coerce_to_column(cfg.lower, col.type),
                _coerce_to_column(cfg.upper, col.type))
    if pa.types.is_timestamp(col.type):
        # 时间范围：取中间 50% 时间窗
        s = pc.sort_indices(col).to_pylist()
        lo = col[s[len(s) // 4]].as_py()
        hi = col[s[(3 * len(s)) // 4]].as_py()
        return lo, hi
    q25 = _scalar_py(pc.quantile(col, q=0.25))
    q75 = _scalar_py(pc.quantile(col, q=0.75))
    return cfg.lower if cfg.lower is not None else q25, \
        cfg.upper if cfg.upper is not None else q75


def run_query(path: str, source_table: pa.Table, cfg: QueryConfig) -> QueryResult:
    res = QueryResult(qtype=cfg.qtype, field=cfg.field, repeats=cfg.repeats)

    if cfg.qtype == "point":
        needle = _resolve_needle(source_table, cfg)
        res.filters = {"field": cfg.field, "==": needle}

        def one():
            tbl = pq.read_table(
                path, columns=[cfg.field],
                filters=[(cfg.field, "==", needle)])
            return tbl.num_rows

        detail = f"点查 {cfg.field} == {needle!r}（filters 下推，只读 1 列）"
    elif cfg.qtype == "range":
        lo, hi = _resolve_bounds(source_table, cfg)
        res.filters = {"field": cfg.field, ">=": lo, "<=": hi}

        def one():
            cols = list(dict.fromkeys([cfg.field, "amount"]))
            tbl = pq.read_table(
                path, columns=cols,
                filters=[(cfg.field, ">=", lo),
                         (cfg.field, "<=", hi)])
            if tbl.num_rows == 0:
                return 0, None
            return tbl.num_rows, _scalar_py(pc.sum(tbl.column("amount")))

        detail = f"范围 {cfg.field} ∈ [{lo}, {hi}]，聚合 sum(amount)"
    elif cfg.qtype == "scan":
        sub = cfg.substring or "warranty"
        res.filters = {"field": cfg.field, "contains": sub}

        def one():
            tbl = pq.read_table(path, columns=[cfg.field])
            # 字面量子串匹配（不做正则解析），NULL 先填空串按不匹配处理
            mask = pc.match_substring(
                pc.coalesce(tbl.column(cfg.field), ""), sub)
            return _scalar_py(pc.sum(pc.cast(mask, pa.int32())))

        detail = f"扫描 {cfg.field} 含子串 {sub!r}（NULL 按不匹配处理）"
    else:
        raise ValueError(f"unknown query type: {cfg.qtype}")

    # 预热一次（不计入），让文件页缓存/代码路径稳定，再做正式重复
    try:
        warm = one()
    except Exception as e:
        raise RuntimeError(f"query warmup failed: {e}") from e

    matched = warm
    for _ in range(cfg.repeats):
        ms, out = _time_once(one)
        res.timings_ms.append(ms)
        if cfg.qtype == "range":
            matched, _ = out
        else:
            matched = out
    res.rows_matched = int(matched or 0)
    res.median_ms = median(res.timings_ms)
    res.detail = detail
    return res
