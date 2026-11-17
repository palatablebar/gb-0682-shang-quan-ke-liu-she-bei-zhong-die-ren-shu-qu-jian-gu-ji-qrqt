# 商圈客流设备重叠与人数区间估计

面向商场经营数据分析员，处理采信、判定、核算、复核与归档。

## 启动

```bash
docker compose up --build -d
```

访问 http://localhost:18251 。初次启动自动运行脏样例，可切换 clean.csv 与 legacy.csv。顶部“运行批次”会重新计算，同源输入复用批次。

## 业务模块

- 设备采信：`/m1_ingest`、`/api/m1_ingest`
- 区域映射：`/m2_mapping`、`/api/m2_mapping`
- 观测分桶：`/m3_versions`、`/api/m3_versions`
- 重叠约束：`/m4_segments`、`/api/m4_segments`
- 人数估界：`/m5_calculate`、`/api/m5_calculate`
- 日汇对账：`/m6_reconcile`、`/api/m6_reconcile`
- 报告归档：`/m7_archive`、`/api/m7_archive`
- 日报封存：`/dayreport`、`/api/dayreport`

## 核心规则

无法识别个人时只输出满足区域交集约束的人数上下界；矛盾观测隔离而非伪造精确人数。完整数值、边界和异常定义见 `docs/DATA-DICTIONARY.md`。

## 日报封存

裁决：**按期封存，不等补发到齐**。截点一到即以当前观测批次封存当日正式日报；晚到的设备补发合并进当日批次后，仅结论变化的时段标待重核，未覆盖时段沿用上一封存的已核结论。

- 上传：`POST /api/dayreport/ingest`（`csv` 或 `sample`）。首个业务日成为基准，之后同日上传均为补发；一次上传跨多个业务日返回 `DAY_SPAN`。
- 幂等：同一补发按内容哈希登记一次，重试返回 `deduped` 且不新增版本；`POST /api/dayreport/<day>/seal` 对同一观测批次重复封存返回既有封存。
- 同一批观测：核对与日汇总都由"基准+已登记补发"合并重放产生，页面、封存与导出始终一致。
- 可见性：每次封存记录补发范围（各批次的覆盖时段）与待重核范围，见页面与 `GET /api/dayreport/seals/<seal_id>/export.csv`。
- 复核：`POST /api/dayreport/seals/<seal_id>/review` 携带当前版本，陈旧复核返回409。

## 验证

```bash
docker compose exec -T app pytest -q
docker compose config --quiet
```

原始样例在 `sample_data/`。测试在 `tests/`。封存文件是单份CSV，含运行清单、计算证据、隔离原因与对账。复核需提交当前版本，陈旧复核返回409。

## 数据与限制

SQLite保存在records命名卷，普通重启与构建不会清除。确认可丢弃本项目数据后，运行 `docker compose down -v` 再启动可重置。单文件上限2MB；不接入外部业务系统；历史规则由本项目样例定义，不代表实际行业标准。
