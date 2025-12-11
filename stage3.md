# 阶段三技术报告：基于 PySpark 与 HBase 的大数据存储与服务化

## 1. 项目概述 (Project Overview)

本项目阶段三（Stage 3）的核心目标是构建一个高性能、可扩展的**游戏数据服务层**。我们利用 **PySpark** 强大的分布式计算能力完成 ETL（抽取、转换、加载）工作，并将清洗后的高价值数据存入 **Apache HBase** (NoSQL 数据库)，以支撑模拟的前端应用进行毫秒级的随机读写和报表查询。

### 技术架构图
```text
[原始数据: CSV] 
      ⬇
[计算层: PySpark ETL] 
    1. 清洗 (Cleaning)
    2. 转换 (RowKey反转)
    3. 聚合 (预计算指标)
      ⬇
[存储层: HBase] 
    1. game_profile (详情表)
    2. dev_analytics (统计表)
      ⬇
[服务层: Data Service]
    1. ID 搜索服务
    2. 开发商仪表盘
````

-----

## 2\. 关键技术栈 (Tech Stack)

  * **数据源**: `data/games_cleaned.csv` (11万+ 条 Steam 游戏清洗数据)
  * **计算引擎**: **PySpark (Apache Spark Python API)**
      * 负责大规模数据的读取、清洗、逻辑转换和聚合计算。
  * **存储引擎**: **Apache HBase**
      * 基于 Hadoop 的列式分布式数据库，提供海量数据的实时读写访问。
  * **数据模型**: 宽表模型 (Wide Column Store)

-----

## 3\. HBase 表结构设计 (Schema Design)

为了满足不同业务场景的查询需求（点查询 vs 范围/聚合查询），我们设计了两张核心业务表。

### 3.1 游戏画像表 (`game_profile`)

**业务场景**：C 端用户在商店页面查看游戏详情（高并发、低延迟点查询）。

  * **RowKey 设计**: `reverse(AppID)` (反转的游戏 ID)
      * **设计理由**: 原始 Steam AppID 是连续递增的数字（如 20200, 20201）。如果直接作为 RowKey，会导致数据顺序写入同一个 RegionServer，引发**热点问题 (Hotspotting)**。通过反转 ID（如 `20200` -\> `00202`），实现了数据的随机散列，使负载均匀分布在集群中。
  * **列族 (Column Families)**:
      * `info`: **静态数据** (Name, Developers, Genres, Release Date)
      * `metrics`: **动态数据** (Price, Owners) —— 实现动静分离，便于独立缓存或压缩。

### 3.2 开发商统计表 (`dev_analytics`)

**业务场景**：B 端商家后台查看业绩报表（聚合分析）。

  * **RowKey 设计**: `Developer_Name` (特殊字符清洗，空格转下划线)
      * **设计理由**: 支持通过开发商名称直接检索。
  * **列族 (Column Families)**:
      * `summary`: **预聚合指标 (Pre-aggregation)**
          * `game_count`: 游戏总数
          * `total_owners`: 总销量/用户量
          * `avg_price`: 平均定价
          * *技术亮点*: 利用 Spark 预先算好存入，避免查询时进行昂贵的实时计算。
      * `product_list`: **倒排索引 (Inverted Index)**
          * 列名: `product_list:{AppID}`
          * 列值: `Game Name`
          * *技术亮点*: 利用 HBase 的宽表特性，将“一对多”关系（一个开发商对应多个游戏）存储在同一行，无需 Join 操作即可获取旗下所有游戏列表。

-----

## 4\. PySpark ETL 流程详解

我们使用 PySpark 实现了从 CSV 到 HBase 的完整数据管道。

### 4.1 数据清洗与转换

```python
# 读取数据
df = spark.read.csv("games_cleaned.csv", header=True, inferSchema=True)

# RowKey 优化：反转 AppID 以防止 HBase 写入热点
df_profile = df.withColumn("rowkey", reverse(col("AppID").cast("string")))
```

### 4.2 聚合计算 (OLAP 逻辑)

针对开发商报表，我们在 Spark 内存中完成了 GroupBy 和 Aggregation 操作：

```python
# 计算核心 KPI：总销量与平均价格
df_summary = df.groupBy("Developers").agg(
    count("AppID").alias("game_count"),
    sum("avg_owners").alias("total_owners"),
    avg("clean_price").alias("avg_price")
)
```

### 4.3 数据加载 (Batch Put)

最终，脚本将处理好的 DataFrame 转换为 HBase Shell 兼容的 `put` 命令脚本，实现了数据的批量导入。

-----

## 5\. 项目成果与验证


通过 HBase Shell 验证数据已物理落盘：

```text
hbase:001:0> get 'game_profile', '00202'
COLUMN                CELL
 info:name            value=Galactic Bowling
 metrics:price        value=19.99
```

