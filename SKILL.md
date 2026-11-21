# Spark 性能分析器

分析 Spark Profiler 生成的 `.sparkprofile` 文件：解析调用树，输出空闲/工作占比、每 tick 耗时、
自耗与累计双榜、每窗口尖峰、按来源（mod/包）聚合、tick 内部拆解。

**v2 起不再按关键词扫字节**（旧版写死 Pipez 过滤词，换个 profile 就是 0 命中）。

## 触发

用户给出 `.sparkprofile` 路径要求分析性能 / 找卡顿元凶 / 对比版本时使用。

## 执行

```bash
python3 spark_analyze.py <文件.sparkprofile> [选项]
```

| 选项 | 说明 |
|---|---|
| `--top N` | 排行榜条数，默认 25 |
| `--filter 文本` | 只看类名/方法名含该文本的条目，可重复给多次 |
| `--thread 名称` | 只看指定线程 |
| `--no-windows` | 不打印每窗口表格 |
| `--no-idle` | 不把 park/sleep 等空闲帧单独扣除 |
| `--tree 文本` | 以匹配节点为根打印调用树 |
| `--depth N` | `--tree` 深度，默认 3 |
| `--csv 路径` | 导出全部节点（self/inclusive/depth/parent） |

常用组合：

```bash
# 整体体检
python3 spark_analyze.py prof.sparkprofile

# 只看某个 mod
python3 spark_analyze.py prof.sparkprofile --filter com.sammy.minersdelight --top 20

# 看某个方法的调用树
python3 spark_analyze.py prof.sparkprofile --tree tickServer --depth 4

# 导出全量给表格复盘
python3 spark_analyze.py prof.sparkprofile --csv nodes.csv
```

## 输出怎么读

1. **头部**：平台、采样模式（执行/分配/锁）、聚合方式（简单平均 / 只统计慢 tick）、间隔、tick 数、TPS。
2. **线程行**：`采样 X / 空闲 Y (Z%) / 工作 W`。空闲是把 `park/sleep` 子树整体扣除后的结果——
   一个健康的 MC 服务端空闲常年在 70~90%，看到 80% 空闲**不代表有问题**，要看下面两项。
3. **tick 工作量 / 平均 ms per tick**：这是真正的负担指标，超过 35~40 ms 就该查了。
4. **每窗口表格**：窗口是 spark 的时间分片（默认 10 s~1 min）。某窗口 >2.5x 中位数会标 `<<< 尖峰`，
   **抓间歇性卡顿就看这一栏**。
5. **自耗 self time**：真正花在方法本体上的时间（不含子调用）。`<- 调用者` 是该节点的主要父帧。
6. **累计 inclusive**：含子调用的总耗时，看结构用（tickServer → tickChildren → …）。
7. **tick 内部拆解**：`MinecraftServer.m_5705_`（tickServer）的直接子节点占比。
8. **按来源**：把 self time 按包聚合，直接看是哪个 mod 在吃时间。

## 格式要点（改脚本时别踩）

- 容器：`SamplerData{1 metadata, 2 threads, 3 class_sources, 6 time_windows}`
- 节点：`ThreadNode{1 name, 3 children, 4 times, 5 children_refs}`、
  `StackTraceNode{3 class, 4 method, 6 line, 7 desc, 8 times[], 9 children_refs}`
- `children_refs` 是**平铺数组里的绝对下标**，且可以指向**前面的**节点（数组按后序排列）。
  用「`ref > 当前下标`」或「`ref` 必须大于父节点」这类校验会把树建错。
- `times` 是**每个时间窗口一个 double**（长度 = `time_windows` 的个数，常见 8~40），
  节点总时间 = 数组求和。单位通常是 **ms**（interval 10000 us 时，一个采样点 = 10 ms）；
  采样模式为分配时单位是 bytes。
- `MinecraftServer.m_5705_` = tickServer，`m_5703_` = tickChildren，`ServerLevel.m_8793_` = tick。
  混淆名在不同版本会变，脚本按方法名 + 所属类双重匹配。

## 抓卡顿的正确姿势

wall-clock 采样会把尖峰摊平，**默认采样抓不到偶发卡死**。要抓就加 tick 过滤：

```bash
/spark profiler start --timeout 300 --only-ticks-over 100
/spark profiler stop
```

这样只有超过 100 ms 的慢 tick 会进样本，分析时 `--top 15` 基本一枪命中。
同时想抓所有线程就用 `--thread *`（或 spark 里的 `--thread *`）。

## 备注

- 旧版脚本（关键词 grep + 写死 Pipez 过滤词）备份在会话工作区 `spark-analyzer-v1-SKILL.md.bak`。
- 分析前先确认 profile 的**采样区间**是否覆盖了出事时间点；不覆盖的话结论只能是「当时是健康的」。
