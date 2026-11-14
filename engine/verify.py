"""读回核对：保证压缩/编码不改变数据语义。

三项硬性校验：
  1. 定点金额 decimal128(18,2) 逐值 Decimal 精确相等（全程不经过 float）；
  2. 时间戳保持微秒精度，逐值相等（含 None）；
  3. 每列空值数量与位置完全一致。
另加全表逐列 array.equals 精确比对与 schema 类型比对。
"""
from __future__ import annotations

from dataclasses import dataclass

import pyarrow as pa
import pyarrow.parquet as pq


@dataclass
class VerifyResult:
    ok: bool
    errors: list[str]
    checks: dict[str, str]


def _schema_types_equal(src: pa.Table, dst: pa.Table) -> str | None:
    for name in src.column_names:
        s = src.schema.field(name).type
        d = dst.schema.field(name).type
        if s != d:
            return f"列 {name} 类型变化: {s} -> {d}"
    return None


def verify_roundtrip(src: pa.Table, path: str,
                     expected: pa.Table | None = None) -> VerifyResult:
    """核对读回数据。

    src 是原始生成表（用于 schema 对照）；expected 是实际写入的表
    （排序方案与 src 行序不同）。为容忍相同排序键下的并列行顺序差异，
    比较前按 (排序键 + id) 做确定性对齐。
    """
    errors: list[str] = []
    checks: dict[str, str] = {}
    dst = pq.read_table(path)
    expected = expected if expected is not None else src

    if dst.num_rows != expected.num_rows:
        errors.append(f"行数变化: {expected.num_rows} -> {dst.num_rows}")

    type_err = _schema_types_equal(src, dst)
    if type_err:
        errors.append(type_err)
    checks["schema"] = "类型逐列一致" if not type_err else "存在类型差异"

    # 确定性对齐：按全部列内容无法排序（含 null/文本），用 id 作为唯一键对齐
    import pyarrow.compute as pc
    order_exp = pc.sort_indices(expected, sort_keys=[("id", "ascending")])
    order_dst = pc.sort_indices(dst, sort_keys=[("id", "ascending")])
    exp_aligned = expected.take(order_exp)
    dst_aligned = dst.take(order_dst)

    # 空值语义：每列 null 计数与空值位置（对齐后比较位置）
    null_mismatch = []
    for name in exp_aligned.column_names:
        s_col, d_col = (exp_aligned.column(name).combine_chunks(),
                        dst_aligned.column(name).combine_chunks())
        if s_col.null_count != d_col.null_count:
            null_mismatch.append(
                f"{name}: null 数 {s_col.null_count} -> {d_col.null_count}")
            continue
        if not pc.is_null(s_col).equals(pc.is_null(d_col)):
            null_mismatch.append(f"{name}: 空值位置不一致")
    if null_mismatch:
        errors.extend(null_mismatch)
        checks["null_semantics"] = "空值不一致"
    else:
        total_nulls = sum(expected.column(n).null_count
                          for n in expected.column_names)
        checks["null_semantics"] = f"逐列 null 计数与位置一致（共 {total_nulls} 个空值）"

    # 精确小数：Decimal 逐值字符串比对，杜绝二进制浮点参与
    amt_src = exp_aligned.column("amount").combine_chunks()
    amt_dst = dst_aligned.column("amount").combine_chunks()
    if amt_dst.type != pa.decimal128(18, 2):
        errors.append(f"amount 精度丢失: 读回类型 {amt_dst.type}")
    else:
        s_vals = amt_src.to_pylist()
        d_vals = amt_dst.to_pylist()
        bad = next((i for i, (a, b) in enumerate(zip(s_vals, d_vals))
                    if a != b or (a is not None and str(a) != str(b))), None)
        if bad is not None:
            errors.append(
                f"amount 第 {bad} 行金额变化: {s_vals[bad]} -> {d_vals[bad]}")
            checks["decimal_exactness"] = "金额不精确"
        else:
            checks["decimal_exactness"] = (
                f"{len(s_vals)} 个 Decimal(18,2) 逐值精确相等，未经过 float 转换")

    # 时间戳精度
    t_src = exp_aligned.column("event_time").combine_chunks()
    t_dst = dst_aligned.column("event_time").combine_chunks()
    if pa.types.is_timestamp(t_dst.type) and t_dst.type.unit != "us":
        errors.append(f"event_time 时间单位变化: us -> {t_dst.type.unit}")
        checks["timestamp_precision"] = "精度被改变"
    elif not t_src.equals(t_dst):
        errors.append("event_time 逐值不相等（微秒级比对失败）")
        checks["timestamp_precision"] = "时间值不一致"
    else:
        checks["timestamp_precision"] = "timestamp[us] 单位与微秒值逐值保持"

    # 全表逐列精确比对（对齐后兜底）
    for name in exp_aligned.column_names:
        if not exp_aligned.column(name).combine_chunks().equals(
                dst_aligned.column(name).combine_chunks()):
            errors.append(f"列 {name} 数据逐值比对不一致")
    checks["full_equality"] = "全列 array.equals 通过" if not any(
        "逐值比对不一致" in e for e in errors) else "存在列不一致"

    return VerifyResult(ok=not errors, errors=errors, checks=checks)
