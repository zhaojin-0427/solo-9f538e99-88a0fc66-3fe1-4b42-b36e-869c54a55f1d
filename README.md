# 环境实验室 · 吸光度转浓度发布系统

把仪器导出的原始吸光度（CSV）按校准曲线换算成质控用浓度明细（CSV），
并用 SQLite 记录每次发布的完整版本。Flask + SQLite，单容器即可启动。

## 快速启动（Docker Compose）

```bash
docker compose up --build -d
```

浏览器打开 <http://localhost:8000>（可用 `LAB_PORT` 改端口）。
数据（SQLite 库与结果文件）保存在命名卷 `lab-data`，容器重启不丢。

停止：

```bash
docker compose down          # 保留数据卷
docker compose down -v       # 连同数据一起删除
```

### 不用 Docker 本地运行

```bash
python3 -m pip install -r requirements.txt
LAB_DATABASE=./data/lab.sqlite3 LAB_RESULTS_DIR=./data/results \
  python3 -m lab.app          # 或 gunicorn --workers 2 lab.wsgi:app
```

测试：`python3 -m pip install -r requirements-dev.txt && python3 -m pytest tests -q`

## 使用流程

### 1. 首次上传（首页表单）

同时上传两个文件并填写**正整数修订号**：

| 文件 | 必需列 | 说明 |
| --- | --- | --- |
| 原始样本 CSV | `样本号`、`UTC时刻`、`吸光度` | 最多 500 行数据（不含表头） |
| 校准 CSV | `起始UTC时刻`、`结束UTC时刻`、`斜率`、`截距` | 完整校准单，可多段 |

表头也接受英文别名（`sample_id, timestamp, absorbance`、
`start, end, slope, intercept`）与常见中文别名；文件须为 UTF-8（允许 BOM）。

校验规则（任一不满足即**整批拒绝**，不产生任何文件或记录）：

- 修订号必须是正整数；
- 吸光度、斜率、截距必须是**有限小数**（拒绝 `nan`、`inf`、`1e999999` 等）；
- UTC 时刻支持 `Z`、`+08:00` 等偏移与空格分隔，统一换算为 UTC；
- 校准区间为**起始含、结束不含**的半开区间，且任意两段不得重叠；
- 每个样本时刻必须**恰好命中一段**（无校准即拒绝）；
- 样本号不得重复；
- 任一浓度为负即拒绝。

浓度 = 斜率 × 吸光度 + 截距，用精确十进制/Fraction 计算，结果 CSV 输出不带
科学计数法、无多余尾零。

结果 CSV 列：`样本号,UTC时刻,吸光度,斜率,截距,浓度`。

### 2. 更正（批次详情页）

只需上传**完整的校准单**和更高的修订号，系统用留存的原始样本重算整批：

- 修订号低于当前修订 → 409 冲突；
- 与当前修订同号且校准内容不同 → 409 冲突；
- 与当前修订同号且内容完全一致（行序无关）→ 幂等成功，不重复发布；
- 修订号更高 → 发布为新版本，旧版本仍可查看、下载。

并发提交时以**提交时（持有 SQLite 写锁后）读取的当前修订**判定，
因此多个请求同时更正只会有一个拿到某个修订号，其余按规则判冲突。

### 3. 查看与下载

- 批次详情页展示当前修订、修订历史、本次校准段与结果预览（前 100 行）；
- `/batches/<token>/revisions/<n>` 查看历史修订；
- `/batches/<token>/download` 或 `/download/<n>` 下载对应结果 CSV。

## 一致性与原子发布

一次发布写入三处：结果 CSV 文件、`versions` 登记行（含结果全文与 SHA-256）、
`results` 明细行。实现方式保证二者要么同时可见、要么同时不可见：

1. 先在结果目录写临时文件并 `fsync`；
2. `BEGIN IMMEDIATE` 写事务内插入批次/样本/版本/明细并切到当前修订，
   随后 `os.replace` 原子落位结果文件；
3. 事务内任一步失败 → 回滚数据库；若本次文件已落位则删除；
4. 下载时同时校验数据库内结果与磁盘文件的大小、SHA-256，二者不一致则该版本拒绝下载；
5. 服务启动时以及首页“清理未发布文件”按钮会扫描结果目录，删除任何未被
   `versions` 登记的文件（崩溃遗留的 `.publish-*.tmp` 等），已发布文件不受影响。

`BEGIN IMMEDIATE` 串行化写事务，再配合结果目录上的 `flock` 文件锁，
在多 gunicorn worker 或多容器共享同一数据卷时同样成立。

## 目录

```
lab/app.py        Flask 路由与错误呈现
lab/parsing.py    CSV 解析、UTC/有限小数校验、浓度精确计算
lab/storage.py    SQLite、原子发布、清理、修订冲突判定
templates/        页面模板
static/           样式
samples/          示例样本、校准与更正校准 CSV
tests/            47 个测试（业务规则、原子性、并发、HTTP）
```

## 配置

| 环境变量 | 默认 | 说明 |
| --- | --- | --- |
| `LAB_PORT` | `8000` | Compose 对外端口 |
| `LAB_SECRET_KEY` | 占位值 | 生产请改为随机值 |
| `LAB_DATABASE` | `/app/data/lab.sqlite3` | SQLite 路径 |
| `LAB_RESULTS_DIR` | `/app/data/results` | 结果文件目录 |
| `WEB_CONCURRENCY` | `2` | gunicorn worker 数 |

首页示例数据：用 `samples/samples.csv` + `samples/calibration.csv` 首次发布，
再用 `samples/calibration-rev2.csv`、修订号 `2` 做一次更正。
