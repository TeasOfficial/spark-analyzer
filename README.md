# spark-analyzer

Minecraft **Spark Profiler** 的 `.sparkprofile` 分析器：解析采样树，定位卡顿元凶。

既可以当 **AI Agent Skill** 用（放到 `~/.agents/skills/spark-analyzer/`），也可以当独立 CLI 用。

## 为什么不用网页版 / 为什么有 v2

spark 自己的网页查看器要开浏览器、要传文件；而很多现成的小工具是**按关键词扫二进制**，
换个 profile 就 0 命中。这个版本直接按 spark 官方
[`spark_sampler.proto`](https://github.com/lucko/spark/blob/master/spark-common/src/main/proto/spark/spark_sampler.proto)
解码完整调用树，所以任何 spark 生成的 profile 都能读。

## 安装

```bash
git clone https://github.com/TeasOfficial/spark-analyzer.git ~/.agents/skills/spark-analyzer
```

Windows 下放到 `C:\Users\<你>\.agents\skills\spark-analyzer`。只需要 Python 3.8+，无第三方依赖。

## 用法

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

常用：

```bash
# 整体体检
python3 spark_analyze.py prof.sparkprofile

# 只看某个 mod
python3 spark_analyze.py prof.sparkprofile --filter com.sammy.minersdelight

# 看 tickServer 的调用树
python3 spark_analyze.py prof.sparkprofile --tree tickServer --depth 4

# 导出全量给表格复盘
python3 spark_analyze.py prof.sparkprofile --csv nodes.csv
```

## 输出怎么读

1. **头部**：平台、采样模式（执行/分配/锁）、聚合方式（简单平均 / 只统计慢 tick）、间隔、tick 数、TPS。
2. **线程行**：`采样 X / 空闲 Y / 工作 W`。空闲是把 `park/sleep` 子树整体扣除的结果——
   健康服务端空闲常年在 70~90%，**80% 空闲不代表有问题**，看下面两项。
3. **tick 工作量 / 平均 ms per tick**：真正的负担指标，超过 35~40 ms 就该查。
4. **每窗口表格**：spark 的时间分片，某窗口 >2.5x 中位数会标 `<<< 尖峰`，**抓间歇性卡顿看这里**。
5. **自耗 self time**：方法本体耗时（不含子调用），`<- 调用者` 是主要父帧。
6. **累计 inclusive**：含子调用，看结构用。
7. **tick 内部拆解**：`MinecraftServer.m_5705_`（tickServer）的直接子节点占比。
8. **按来源**：self time 按包聚合，直接看哪个 mod 在吃时间。

## 抓间歇性卡顿的正确姿势

wall-clock 采样会把尖峰摊平，默认参数**抓不到偶发卡死**。要抓就加 tick 过滤：

```
/spark profiler start --timeout 300 --only-ticks-over 100
/spark profiler stop
```

只有超过 100 ms 的慢 tick 会进样本，分析时 `--top 15` 基本一枪命中。

## 格式要点（改脚本前必读）

- 容器：`SamplerData{1 metadata, 2 threads, 3 class_sources, 6 time_windows}`
- 节点：`ThreadNode{1 name, 3 children, 4 times, 5 children_refs}`、
  `StackTraceNode{3 class, 4 method, 6 line, 7 desc, 8 times[], 9 children_refs}`
- `children_refs` 是平铺数组里的**绝对下标**，且可以指向**前面的**节点（后序排列）。
  用「`ref > 当前下标`」这类校验会把树建错——这是最容易踩的坑。
- `times` 是**每个时间窗口一个 double**（长度 = `time_windows` 个数，常见 8~40），
  节点总时间 = 数组求和。单位通常 **ms**（interval 10000 us 时一个采样点 = 10 ms）；
  分配模式下是 bytes。
- `MinecraftServer.m_5705_` = tickServer，`m_5703_` = tickChildren，`ServerLevel.m_8793_` = tick。
  混淆名随版本变化，脚本按「方法名 + 所属类」双重匹配。

## 输出示例

```
采样区间    : 502.4 秒
覆盖 tick   : 9866  (平均 19.64 TPS)
采样总时间  : 478.74 s   (单位 ms，线程 1 个)

线程 'Server thread'   采样 478.74 s   空闲 385.41 s (80.5%)   工作 93.33 s (19.5%)
tick 工作量 87.39 s   平均 8.86 ms/tick   (tick 预算 50 ms)
  每窗口 tick 工作量:
     w0     12.46 s   平均 10.10 ms/tick
     ...
tick 内部拆解 (net.minecraft.server.MinecraftServer.m_5705_ = 87.39 s)
    DedicatedServer.m_5703_                           81.73 s   93.5%
    ForgeEventFactory.onPreServerTick                  3.09 s    3.5%
```
