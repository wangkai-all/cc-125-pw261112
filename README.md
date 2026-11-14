# Parquet 数据文件优化实验室

把同一份明细数据按不同 **压缩算法 × 字典/列编码 × 行组大小 × 排序** 写成
Parquet，对比每种方案的文件大小、写入耗时与三类查询的实际表现，并可点击
查看每个方案各列的真实编码情况。所有方案写出后都会读回核对，失败或核对
不通过的方案不会进入推荐结果。

## 它保证什么

- **精确小数不被压缩改变**：金额列固定 `decimal128(18,2)`，用 `Decimal`
  构造、逐值字符串级比对，全程不经过 `float`。
- **时间精度不降级**：`timestamp[us]` 读回后校验单位与微秒值逐值相等。
- **空值语义不变**：每列 null 计数与空值位置（排序方案按 id 对齐后）完全一致。
- **不支持的组合明确报原因**：LZO（当前 pyarrow 未编译）、字符串列 RLE、
  字符串列 BYTE_STREAM_SPLIT 等故意构造的方案会记录引擎原始报错与中文说明，
  并从图表、推荐中排除。
- **结论不看单次偶然最快**：每条查询先预热 1 次（不计入），再重复执行 N 次，
  页面展示每次原始耗时与中位数，推荐与排序均基于中位数。
- **排序是独立方案**：排序方案单独列出并记录排序耗时（`sort_ms`），与未排序
  的同参数方案并列比较。
- **每项展示都有血缘**：manifest 记录生成器参数指纹、数据内容指纹、完整写入
  参数与查询配置；源数据（Arrow IPC）、每个 parquet、manifest 均可下载，
  用同种子同参数重跑可复现。

## 样本数据（本地生成器 `engine/datagen.py`）

| 列 | 类型 | 特征 |
|---|---|---|
| `category` | string | 高重复类别（6 种，带倾斜），字典编码友好 |
| `region` | string | 低基数，含 8% 空值 |
| `id` | int64 | 几乎不重复标识（0.2% 重复），delta 友好 |
| `amount` | decimal128(18,2) | 定点金额，精确两位小数 |
| `quantity` | int32 | 整型计数 |
| `score` | double | 浮点，BYTE_STREAM_SPLIT 目标 |
| `remark` | string | 长文本（8–20 个词），15% 空值 |
| `event_time` | timestamp[us] | 微秒精度，5% 空值 |
| `is_active` | bool | 布尔（RLE 适用） |
| `ref_code` | string | 高基数短字符串 |

同 `seed` 下输出逐行稳定。

## 快速开始

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt

# 方式一：Web 页面
.venv/bin/python server.py            # http://127.0.0.1:8000

# 方式二：纯命令行重跑
.venv/bin/python run_cli.py --rows 200000 --repeats 5
```

页面上可以：

1. 调整行数、种子、各列空值率；
2. 勾选压缩算法（snappy / zstd / gzip / lz4 / brotli / none）、
   编码 profile、行组大小，决定是否加入排序对照；
3. 增删查询、切换查询字段、改过滤范围/目标值/子串与重复次数：
   - 点查：可直接填目标值（留空则自动从数据采样）；
   - 范围聚合：填 `下界,上界`（留空默认取 25%–75% 分位，时间列支持 ISO 时间窗）；
   - 文本扫描：自定义匹配子串；
4. 查看语义核对结果、推荐方案、可排序明细表与条形图（指标条按本次查询
   动态生成，每条查询都有独立的中位数指标与推荐卡）；
5. 点「查看列编码」看每个方案逐列的物理类型、编码、字典页、统计信息与
   压缩前后字节数；失败方案点「原因」看引擎报错。

## 编码 profile 含义（`engine/schemes.py`）

| profile | 实际 write_table 参数 |
|---|---|
| `defaults` | 全列字典编码 + pyarrow 默认兜底 |
| `no_dictionary` | `use_dictionary=False`，全 PLAIN |
| `delta` | 整型 `DELTA_BINARY_PACKED`，字节串 `DELTA_BYTE_ARRAY` |
| `byte_stream_split` | 数值/定长列 BSS（pyarrow 要求同时关闭字典） |
| `selective_dict` | 仅 `category/region` 字典，`id` 走 delta |
| `mixed` | 低基数字典 + 数值 BSS + 整型 delta + 长文本 DELTA_LENGTH_BYTE_ARRAY |

## 查询设计（`engine/queries.py`）

- **point（点查）**：`read_table(columns, filters=[(col,"==",needle)])`，
  谓词下推、只读一列；目标值可指定或自动从数据采样。
- **range（范围聚合）**：过滤区间（默认取 25%–75% 分位，时间列取中间时间窗）
  + `sum(amount)`，考察行组统计裁剪与向量化聚合。
- **scan（文本扫描）**：整列读取后子串匹配，NULL 按不匹配处理，考察字符串页
  解码成本。
- 每次重复都重新打开文件，避免页缓存把一次偶然值当成结论。

## 产物结构

```
artifacts/<run_id>/
├── manifest.json            # 完整血缘：数据指纹、方案参数、全部计时与核对结果
└── data/
    ├── _source.arrow        # 生成的源数据（Arrow IPC，无损）
    ├── _source_preview.json # 前 20 行预览
    └── <scheme>.parquet     # 每个成功方案一个文件（页面可下载）
```

## 测试

```bash
.venv/bin/python -m pytest tests/ -q
```

覆盖：生成确定性、schema/空值、六个 profile 的读回核对、Decimal 不经过 float、
排序方案对齐核对、不支持组合必报错、查询重复次数与中位数、空值语义保持。

## 目录

```
engine/      数据生成、方案矩阵、读回核对、查询基准、编排器
web/         无依赖前端（原生 HTML/CSS/JS + SVG 条形图）
server.py    标准库 HTTP 服务（静态托管 + JSON API + 下载）
run_cli.py   命令行重跑入口
tests/       语义测试
```
