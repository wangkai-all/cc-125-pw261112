"""本地合成数据生成器。

生成一份覆盖典型 Parquet 编码场景的明细数据：
  - category     高重复类别（字典编码友好）
  - region       低基数类别，含空值
  - id           几乎不重复的标识（字典编码不友好，适合 delta）
  - amount       定点金额 decimal128(18,2)，精确小数
  - quantity     整型
  - score        double，适合 BYTE_STREAM_SPLIT
  - remark       长文本，含空值
  - event_time   微秒精度时间戳，含空值
  - is_active    布尔
  - ref_code     高基数短字符串（混合重复度）

同一种子 (seed) 下输出逐字节稳定，保证重跑可复现。
"""
from __future__ import annotations

import hashlib
import json
import random
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from decimal import Decimal

import pyarrow as pa

CATEGORIES = ["electronics", "grocery", "apparel", "books", "home", "sports"]
REGIONS = ["north", "south", "east", "west", None]
REMARK_WORDS = (
    "promotion clearance bundle warranty refund exchange "
    "vip member bulk order seasonal limited stock replenishment "
    "logistics delay quality inspection customer follow up"
).split()


@dataclass(frozen=True)
class DatasetSpec:
    rows: int = 200_000
    seed: int = 20261114
    region_null_pct: float = 0.08
    remark_null_pct: float = 0.15
    event_null_pct: float = 0.05

    def fingerprint(self) -> str:
        payload = json.dumps(asdict(self), sort_keys=True).encode()
        return hashlib.sha256(payload).hexdigest()[:16]


def _build_columns(spec: DatasetSpec):
    rng = random.Random(spec.seed)
    n = spec.rows
    base_time = datetime(2026, 1, 1, 0, 0, 0)

    category, region, id_col = [], [], []
    amount, quantity, score = [], [], []
    remark, event_time, is_active, ref_code = [], [], [], []

    for i in range(n):
        # 高重复类别：带轻微倾斜
        category.append(CATEGORIES[rng.choices(range(6), weights=[5, 4, 3, 2, 2, 1])[0]])
        region.append(REGIONS[rng.randrange(len(REGIONS))]
                      if rng.random() >= spec.region_null_pct else None)
        # 几乎不重复标识，仅少量重复
        id_col.append(i if rng.random() > 0.002 else rng.randrange(n))

        # 定点金额：始终保留两位小数，用 Decimal 构造杜绝浮点误差
        yuan = rng.randrange(1, 50_000)
        cents = rng.randrange(0, 100)
        amount.append(Decimal(f"{yuan}.{cents:02d}"))
        quantity.append(rng.randrange(1, 200))
        score.append(round(rng.random() * 100, 6))

        if rng.random() < spec.remark_null_pct:
            remark.append(None)
        else:
            k = rng.randrange(8, 20)
            remark.append(" ".join(rng.choice(REMARK_WORDS) for _ in range(k)))

        if rng.random() < spec.event_null_pct:
            event_time.append(None)
        else:
            event_time.append(base_time + timedelta(
                microseconds=rng.randrange(0, 60 * 24 * 3600 * 1_000_000)))

        is_active.append(rng.random() > 0.3)
        ref_code.append(f"REF-{rng.randrange(0, n // 4):08d}")

    return {
        "category": pa.array(category, pa.string()),
        "region": pa.array(region, pa.string()),
        "id": pa.array(id_col, pa.int64()),
        "amount": pa.array(amount, pa.decimal128(18, 2)),
        "quantity": pa.array(quantity, pa.int32()),
        "score": pa.array(score, pa.float64()),
        "remark": pa.array(remark, pa.string()),
        "event_time": pa.array(event_time, pa.timestamp("us")),
        "is_active": pa.array(is_active, pa.bool_()),
        "ref_code": pa.array(ref_code, pa.string()),
    }


def generate(spec: DatasetSpec) -> pa.Table:
    table = pa.table(_build_columns(spec))
    return table.replace_schema_metadata({
        b"dataset_spec": json.dumps(asdict(spec), sort_keys=True).encode(),
        b"fingerprint": spec.fingerprint().encode(),
    })


def table_fingerprint(table: pa.table) -> str:
    """对 Arrow 表内容做稳定指纹（用于读回核对之外的血缘记录）。"""
    h = hashlib.sha256()
    for name in table.column_names:
        col = table.column(name)
        h.update(name.encode())
        h.update(str(col.type).encode())
        h.update(b"\x1f".join(repr(v).encode() for v in col.slice(0, min(len(col), 5000)).to_pylist()))
    return h.hexdigest()[:16]
