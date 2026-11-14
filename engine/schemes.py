"""Parquet 写入方案定义。

一个方案 (Scheme) 完整描述一次 write_table 调用的参数：
  - compression / compression_level
  - 字典编码开关与列级编码 profile
  - row_group_size
  - 排序（排序作为独立方案，额外记录排序耗时）

profile 只描述“想怎么编码”，真正落到 pyarrow 参数的转换在 build_write_kwargs，
不支持的组合在这里或写入时抛出带原因的错误，由 runner 捕获并标记为 failed。
"""
from __future__ import annotations

from dataclasses import dataclass, field, asdict
from typing import Any

# pyarrow 26 当前构建可用的压缩（lzo 不可用，见 INVALID_SCHEMES）
COMPRESSIONS = ["snappy", "zstd", "gzip", "lz4", "brotli", "none"]
DEFAULT_LEVELS = {"zstd": 3, "gzip": 6, "brotli": 6}

PROFILES = [
    "defaults",          # 全列字典 + pyarrow 默认 PLAIN 兜底
    "no_dictionary",     # 关闭字典，全部 PLAIN
    "delta",             # 整型 DELTA_BINARY_PACKED，字节串 DELTA_BYTE_ARRAY
    "byte_stream_split", # 数值列 BSS（需关字典）
    "selective_dict",    # 仅低基数列字典，id 走 delta
    "mixed",             # 组合：类别字典 + delta + BSS + 长文本 delta-length
]

# 每个 profile 适用的列（按表结构固定，便于解释）
INT_COLS = ["id", "quantity"]
BSS_COLS = ["id", "quantity", "amount", "score"]
STRING_COLS = ["category", "region", "remark", "ref_code"]
LOW_CARD_COLS = ["category", "region"]


@dataclass(frozen=True)
class Scheme:
    name: str
    compression: str = "snappy"
    profile: str = "defaults"
    row_group_size: int = 0          # 0 表示 pyarrow 默认（单 row group）
    sort_columns: tuple[str, ...] = ()
    compression_level: int | None = None
    expect_invalid: bool = False     # 故意构造的不支持组合
    invalid_reason_hint: str = ""

    def key(self) -> str:
        sort = "-".join(self.sort_columns) if self.sort_columns else "unsorted"
        return (f"{self.name}|{self.compression}|{self.profile}|"
                f"rg{self.row_group_size}|{sort}")

    def params_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["sort_columns"] = list(self.sort_columns)
        return d


def build_write_kwargs(scheme: Scheme) -> dict[str, Any]:
    """把方案翻译成 pyarrow.parquet.write_table 的 kwargs。

    不支持的组合直接抛 ValueError（英文原因来自引擎，中文解释在 runner 层补充）。
    """
    if scheme.compression not in COMPRESSIONS:
        raise ValueError(
            f"compression '{scheme.compression}' is not supported by this "
            f"pyarrow build (available: {', '.join(COMPRESSIONS)})")
    if scheme.profile not in PROFILES:
        raise ValueError(f"unknown encoding profile '{scheme.profile}'")

    kwargs: dict[str, Any] = {
        "compression": None if scheme.compression == "none" else scheme.compression,
        "write_statistics": True,
        "data_page_version": "2.0",
    }
    level = scheme.compression_level
    if level is None:
        level = DEFAULT_LEVELS.get(scheme.compression)
    if level is not None:
        kwargs["compression_level"] = level
    if scheme.row_group_size and scheme.row_group_size > 0:
        kwargs["row_group_size"] = scheme.row_group_size

    p = scheme.profile
    if p == "defaults":
        kwargs["use_dictionary"] = True
    elif p == "no_dictionary":
        kwargs["use_dictionary"] = False
    elif p == "delta":
        kwargs["use_dictionary"] = False
        kwargs["column_encoding"] = {
            **{c: "DELTA_BINARY_PACKED" for c in INT_COLS},
            **{c: "DELTA_BYTE_ARRAY" for c in STRING_COLS},
        }
    elif p == "byte_stream_split":
        kwargs["use_dictionary"] = False
        kwargs["use_byte_stream_split"] = BSS_COLS
    elif p == "selective_dict":
        kwargs["use_dictionary"] = LOW_CARD_COLS
        kwargs["column_encoding"] = {"id": "DELTA_BINARY_PACKED"}
    elif p == "mixed":
        kwargs["use_dictionary"] = LOW_CARD_COLS
        kwargs["use_byte_stream_split"] = ["score", "amount"]
        kwargs["column_encoding"] = {
            "id": "DELTA_BINARY_PACKED",
            "quantity": "DELTA_BINARY_PACKED",
            "remark": "DELTA_LENGTH_BYTE_ARRAY",
        }
    return kwargs


def default_plan(compressions: list[str] | None = None,
                 profiles: list[str] | None = None,
                 row_group_sizes: list[int] | None = None,
                 add_sorted: bool = True) -> list[Scheme]:
    """构造默认对比矩阵：压缩 × 编码 profile × 行组大小 + 排序对照。"""
    compressions = compressions or ["snappy", "zstd", "gzip"]
    profiles = profiles or PROFILES
    row_group_sizes = row_group_sizes or [0, 50_000]
    schemes: list[Scheme] = []
    for comp in compressions:
        for prof in profiles:
            for rg in row_group_sizes:
                schemes.append(Scheme(
                    name=f"{comp}-{prof}-rg{rg or 'auto'}",
                    compression=comp, profile=prof, row_group_size=rg))
    if add_sorted:
        # 排序前后作为独立方案：固定 zstd + defaults，两种排序键
        for sort_cols, label in [(("category",), "sort-category"),
                                 (("id",), "sort-id"),
                                 (("event_time",), "sort-time")]:
            schemes.append(Scheme(
                name=f"zstd-defaults-{label}",
                compression="zstd", profile="defaults",
                row_group_size=50_000, sort_columns=sort_cols))
    schemes.extend(invalid_demo_schemes())
    return schemes


def invalid_demo_schemes() -> list[Scheme]:
    """每次运行附带的不支持组合，用于验证失败原因上报与推荐过滤。"""
    return [
        Scheme(name="invalid-lzo", compression="lzo",
               expect_invalid=True,
               invalid_reason_hint="当前 pyarrow 构建未编译 LZO 支持"),
        Scheme(name="invalid-rle-on-string", compression="snappy",
               profile="no_dictionary", expect_invalid=True,
               invalid_reason_hint="RLE 编码仅支持 BOOLEAN 列"),
        Scheme(name="invalid-bss-on-string", compression="snappy",
               profile="no_dictionary", expect_invalid=True,
               invalid_reason_hint="BYTE_STREAM_SPLIT 不支持字节串列"),
    ]


def write_kwargs_for_invalid(scheme: Scheme) -> dict[str, Any]:
    """为故意失败的方案构造必然报错的 kwargs。"""
    if scheme.name == "invalid-lzo":
        return {"compression": "lzo", "use_dictionary": True}
    if scheme.name == "invalid-rle-on-string":
        return {"compression": "snappy", "use_dictionary": False,
                "column_encoding": {"category": "RLE"}}
    if scheme.name == "invalid-bss-on-string":
        return {"compression": "snappy", "use_dictionary": False,
                "use_byte_stream_split": ["remark"]}
    raise ValueError(f"not an invalid demo scheme: {scheme.name}")
