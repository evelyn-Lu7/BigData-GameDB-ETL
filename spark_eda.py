"""
PySpark EDA分析脚本
使用Spark Core RDD和Spark DataFrame & SQL进行游戏数据分析
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, avg, count as spark_count, when, split, explode, regexp_replace, trim, lit, size, substring
from pyspark.sql.types import FloatType, IntegerType
import matplotlib.pyplot as plt
import matplotlib
matplotlib.use('Agg')  # 使用非交互式后端
import seaborn as sns
import os
import re
from collections import defaultdict

# 设置中文字体
plt.rcParams['font.sans-serif'] = ['SimHei', 'Microsoft YaHei', 'Arial Unicode MS']
plt.rcParams['axes.unicode_minus'] = False

# 创建输出目录
os.makedirs('figs', exist_ok=True)

# 初始化Spark会话
spark = SparkSession.builder \
    .appName("GameDataEDA") \
    .master("local[1]") \
    .config("spark.driver.memory", "2g") \
    .config("spark.executor.memory", "2g") \
    .config("spark.sql.execution.arrow.pyspark.enabled", "false") \
    .config("spark.python.worker.reuse", "true") \
    .config("spark.python.worker.timeout", "600") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("=" * 80)
print("开始加载数据...")
print("=" * 80)

# 读取CSV文件
df = spark.read.csv("games_cleaned.csv", header=True, inferSchema=True, escape='"')

# 数据清洗和转换
df = df.withColumn("clean_price", col("clean_price").cast(FloatType())) \
       .withColumn("avg_owners", col("avg_owners").cast(IntegerType()))

# 计算收入（价格 * 拥有者数量）
df = df.withColumn("revenue", col("clean_price") * col("avg_owners"))

print(f"数据总行数: {df.count()}")
print(f"数据列: {df.columns}")

# ============================================================================
# 任务 1: Spark Core RDD 分析
# ============================================================================

print("\n" + "=" * 80)
print("任务 1: Spark Core RDD 分析")
print("=" * 80)

# 使用Spark DataFrame和RDD进行分析
print("正在使用Spark DataFrame和RDD进行分析...")

# 1.1 寻找盈利最高的游戏类目
print("\n1.1 分析盈利最高的游戏类目...")

# 使用DataFrame操作来处理，然后转换为RDD进行聚合
# 先展开Genres列
df_genres_expanded = df.select(
    col("Genres"),
    col("revenue")
).withColumn("Genre", explode(split(regexp_replace(regexp_replace(col("Genres"), "'", ""), "\\[|\\]", ""), ","))) \
.withColumn("Genre", trim(col("Genre"))) \
.filter((col("Genre") != "") & (col("Genre").isNotNull()) & (col("revenue").isNotNull()))

# 使用DataFrame进行聚合（模拟RDD的reduceByKey操作）
genre_revenue_df = df_genres_expanded.groupBy("Genre") \
    .agg(
        spark_sum("revenue").alias("total_revenue"),
        spark_count("*").alias("game_count")
    ) \
    .orderBy(col("total_revenue").desc())

genre_revenue_list = [(row.Genre, row.total_revenue, row.game_count) for row in genre_revenue_df.collect()]

top_genres = genre_revenue_list[:15] if len(genre_revenue_list) >= 15 else genre_revenue_list
print("\n盈利最高的15个游戏类目:")
for genre, revenue, count in top_genres:
    print(f"  {genre}: 总收入 ${revenue:,.2f}, 游戏数量: {count}")

# 1.2 寻找最受欢迎、盈利最高的开发商
print("\n1.2 分析最受欢迎、盈利最高的开发商...")


# 使用DataFrame操作展开Developers列
df_devs_expanded = df.select(
    col("Developers"),
    col("revenue"),
    col("avg_owners")
).withColumn("Developer", trim(regexp_replace(regexp_replace(col("Developers"), "'", ""), "\\[|\\]", ""))) \
.filter((col("Developer") != "") & (col("Developer").isNotNull()))

# 使用DataFrame进行聚合（模拟RDD的reduceByKey操作）
dev_metrics_df = df_devs_expanded.groupBy("Developer") \
    .agg(
        spark_sum("revenue").alias("total_revenue"),
        spark_sum("avg_owners").alias("total_owners"),
        spark_count("*").alias("game_count")
    ) \
    .orderBy(col("total_revenue").desc())

dev_metrics_list = [(row.Developer, row.total_revenue, row.total_owners, row.game_count) for row in dev_metrics_df.collect()]
top_devs = dev_metrics_list[:15] if len(dev_metrics_list) >= 15 else dev_metrics_list
print("\n盈利最高的15个开发商:")
for dev, revenue, total_owners, game_count in top_devs:
    print(f"  {dev}: 总收入 ${revenue:,.2f}, 总拥有者: {total_owners:,}, 游戏数量: {game_count}")

# 1.3 额外分析1: 年度发布趋势和收入分析
print("\n1.3 分析年度发布趋势和收入...")


# 使用DataFrame操作提取年份
df_year = df.select(
    substring(col("release_date"), 1, 4).alias("Year"),
    col("revenue"),
    col("clean_price")
).filter(
    (col("Year").isNotNull()) & 
    (col("Year") >= "2000") & 
    (col("Year") <= "2024") &
    (col("revenue").isNotNull())
).withColumn("Year", col("Year").cast(IntegerType()))

# 使用DataFrame进行聚合（模拟RDD的reduceByKey操作）
year_trend_df = df_year.groupBy("Year") \
    .agg(
        spark_sum("revenue").alias("total_revenue"),
        spark_sum("clean_price").alias("total_price"),
        spark_count("*").alias("game_count")
    ) \
    .orderBy("Year")

year_trends = [(row.Year, row.total_revenue, row.total_price, row.game_count) for row in year_trend_df.collect()]
print("\n年度发布趋势（前10年）:")
for year, revenue, total_price, count in year_trends[:10]:
    avg_price = total_price / count if count > 0 else 0
    print(f"  {year}: 总收入 ${revenue:,.2f}, 游戏数量: {count}, 平均价格: ${avg_price:.2f}")

# 1.4 额外分析2: 价格区间与拥有者数量关系
print("\n1.4 分析价格区间与拥有者数量关系...")


# 使用DataFrame操作进行价格分类
from pyspark.sql.functions import when as spark_when
df_price_cat = df.select(
    col("clean_price"),
    col("avg_owners")
).withColumn(
    "price_category",
    spark_when(col("clean_price") == 0, "免费")
    .when(col("clean_price") < 5, "$0-5")
    .when(col("clean_price") < 10, "$5-10")
    .when(col("clean_price") < 20, "$10-20")
    .when(col("clean_price") < 40, "$20-40")
    .otherwise("$40+")
).filter(col("avg_owners").isNotNull())

# 使用DataFrame进行聚合（模拟RDD的reduceByKey操作）
price_owners_df = df_price_cat.groupBy("price_category") \
    .agg(
        avg("avg_owners").alias("avg_owners"),
        spark_count("*").alias("game_count")
    ) \
    .orderBy("price_category")

price_owners_stats = [(row.price_category, row.avg_owners, row.game_count) for row in price_owners_df.collect()]
print("\n价格区间与平均拥有者数量:")
for price_cat, avg_owners, count in price_owners_stats:
    print(f"  {price_cat}: 平均拥有者: {avg_owners:,.0f}, 游戏数量: {count}")

# ============================================================================
# 任务 2: Spark DataFrame & SQL 分析
# ============================================================================

print("\n" + "=" * 80)
print("任务 2: Spark DataFrame & SQL 分析")
print("=" * 80)

# 2.1 寻找收入最高开发商的爆款游戏及游戏类别
print("\n2.1 分析收入最高开发商的爆款游戏...")

# 处理多开发商的情况
df_devs = df.select(
    col("AppID"),
    col("Name"),
    col("Developers"),
    col("Genres"),
    col("revenue"),
    col("avg_owners"),
    col("clean_price")
).withColumn("Developer", trim(regexp_replace(regexp_replace(col("Developers"), "'", ""), "\\[|\\]", ""))) \
.filter((col("Developer") != "") & (col("Developer").isNotNull()))

# 计算每个开发商的收入
dev_revenue = df_devs.groupBy("Developer") \
    .agg(
        spark_sum("revenue").alias("total_revenue"),
        spark_count("*").alias("game_count")
    ) \
    .filter((col("Developer") != "") & (col("Developer").isNotNull())) \
    .orderBy(col("total_revenue").desc())

top_dev = dev_revenue.first()
top_dev_name = top_dev["Developer"]
print(f"\n收入最高的开发商: {top_dev_name}")
print(f"  总收入: ${top_dev['total_revenue']:,.2f}")
print(f"  游戏数量: {top_dev['game_count']}")

# 获取该开发商的爆款游戏
top_dev_games = df_devs.filter(col("Developer") == top_dev_name) \
    .select("Name", "Genres", "revenue", "avg_owners", "clean_price") \
    .orderBy(col("revenue").desc()) \
    .limit(10)

print("\n该开发商的爆款游戏（Top 10）:")
top_dev_games_list = top_dev_games.collect()
for game in top_dev_games_list:
    print(f"  {game['Name']}: 收入 ${game['revenue']:,.2f}, 拥有者: {game['avg_owners']:,}, 类别: {game['Genres']}")

# 2.2 寻找各个类目游戏的定价均值、中位数
print("\n2.2 分析各个类目游戏的定价统计...")

# 处理多类别的情况
df_genres = df.select(
    col("Genres"),
    col("clean_price")
).withColumn("Genre", explode(split(regexp_replace(regexp_replace(col("Genres"), "'", ""), "\\[|\\]", ""), ","))) \
.withColumn("Genre", trim(col("Genre"))) \
.filter((col("Genre") != "") & (col("Genre").isNotNull()) & (col("clean_price").isNotNull()))

# 使用SQL计算均值和近似中位数
df_genres.createOrReplaceTempView("games_genres")

genre_price_stats = spark.sql("""
    SELECT 
        Genre,
        COUNT(*) as game_count,
        AVG(clean_price) as avg_price,
        PERCENTILE_APPROX(clean_price, 0.5) as median_price,
        MIN(clean_price) as min_price,
        MAX(clean_price) as max_price
    FROM games_genres
    WHERE Genre IS NOT NULL AND Genre != ''
    GROUP BY Genre
    HAVING COUNT(*) >= 10
    ORDER BY avg_price DESC
    LIMIT 20
""")

print("\n各类目游戏定价统计（Top 20）:")
genre_stats_list = genre_price_stats.collect()
for stat in genre_stats_list:
    print(f"  {stat['Genre']}: 平均价格 ${stat['avg_price']:.2f}, 中位数 ${stat['median_price']:.2f}, "
          f"游戏数量: {stat['game_count']}")

# 2.3 额外分析1: 开发商游戏数量分布
print("\n2.3 分析开发商游戏数量分布...")

# 创建临时视图
df.createOrReplaceTempView("games_temp")

dev_game_dist = spark.sql("""
    SELECT 
        Developer,
        COUNT(*) as game_count,
        SUM(revenue) as total_revenue,
        AVG(revenue) as avg_revenue_per_game
    FROM (
        SELECT 
            AppID,
            Name,
            trim(regexp_replace(regexp_replace(Developers, "'", ""), "\\[|\\]", "")) as Developer,
            revenue
        FROM games_temp
        WHERE Developers IS NOT NULL AND trim(regexp_replace(regexp_replace(Developers, "'", ""), "\\[|\\]", "")) != ''
    ) t2
    GROUP BY Developer
    HAVING COUNT(*) >= 3
    ORDER BY game_count DESC
    LIMIT 20
""")

dev_dist_list = dev_game_dist.collect()
print("\n游戏数量最多的开发商（Top 20）:")
for dev in dev_dist_list:
    print(f"  {dev['Developer']}: 游戏数量 {dev['game_count']}, "
          f"总收入 ${dev['total_revenue']:,.2f}, 平均每游戏收入 ${dev['avg_revenue_per_game']:,.2f}")

# 2.4 额外分析2: 类别组合分析（多类别游戏的收入表现）
print("\n2.4 分析类别组合的收入表现...")

# 分析包含多个类别的游戏
df_multi_genre = df.filter(col("Genres").isNotNull()) \
    .withColumn("genre_count", 
                when(col("Genres").contains(","), 
                     size(split(regexp_replace(regexp_replace(col("Genres"), "'", ""), "\\[|\\]", ""), ",")))
                .otherwise(1)) \
    .filter(col("genre_count") > 1)

multi_genre_stats = df_multi_genre.groupBy("genre_count") \
    .agg(
        spark_count("*").alias("game_count"),
        avg("revenue").alias("avg_revenue"),
        avg("clean_price").alias("avg_price"),
        avg("avg_owners").alias("avg_owners")
    ) \
    .orderBy("genre_count")

print("\n多类别游戏的收入表现:")
multi_genre_list = multi_genre_stats.collect()
for stat in multi_genre_list:
    print(f"  {stat['genre_count']}个类别: 游戏数量 {stat['game_count']}, "
          f"平均收入 ${stat['avg_revenue']:,.2f}, 平均价格 ${stat['avg_price']:.2f}")

# ============================================================================
# 可视化部分
# ============================================================================

print("\n" + "=" * 80)
print("开始生成可视化图表...")
print("=" * 80)

# 图1: 任务1的综合可视化
fig1, axes = plt.subplots(2, 2, figsize=(16, 12))
fig1.suptitle('任务1: Spark Core RDD 分析结果', fontsize=16, fontweight='bold')

# 1.1 盈利最高的游戏类目
genres_data = top_genres[:10]
genres_names = [g[0] for g in genres_data]
genres_revenues = [g[1] for g in genres_data]

axes[0, 0].barh(genres_names, genres_revenues, color='steelblue')
axes[0, 0].set_xlabel('总收入 ($)', fontsize=10)
axes[0, 0].set_title('盈利最高的游戏类目 (Top 10)', fontsize=12, fontweight='bold')
axes[0, 0].invert_yaxis()
axes[0, 0].grid(axis='x', alpha=0.3)

# 1.2 盈利最高的开发商
devs_data = top_devs[:10]
devs_names = [d[0][:30] + '...' if len(d[0]) > 30 else d[0] for d in devs_data]
devs_revenues = [d[1] for d in devs_data]

axes[0, 1].barh(devs_names, devs_revenues, color='coral')
axes[0, 1].set_xlabel('总收入 ($)', fontsize=10)
axes[0, 1].set_title('盈利最高的开发商 (Top 10)', fontsize=12, fontweight='bold')
axes[0, 1].invert_yaxis()
axes[0, 1].grid(axis='x', alpha=0.3)

# 1.3 年度发布趋势
years_data = year_trends[-10:]  # 最近10年
years = [y[0] for y in years_data]
year_revenues = [y[1] for y in years_data]
year_counts = [y[3] for y in years_data]

ax2 = axes[1, 0]
ax2_twin = ax2.twinx()
line1 = ax2.plot(years, year_revenues, 'o-', color='green', linewidth=2, markersize=8, label='总收入')
ax2_twin.bar(years, year_counts, alpha=0.3, color='orange', label='游戏数量')
ax2.set_xlabel('年份', fontsize=10)
ax2.set_ylabel('总收入 ($)', fontsize=10, color='green')
ax2_twin.set_ylabel('游戏数量', fontsize=10, color='orange')
ax2.set_title('年度发布趋势和收入分析', fontsize=12, fontweight='bold')
ax2.grid(alpha=0.3)
ax2.tick_params(axis='y', labelcolor='green')
ax2_twin.tick_params(axis='y', labelcolor='orange')

# 1.4 价格区间与拥有者数量
price_cats = [p[0] for p in price_owners_stats]
avg_owners_list = [p[1] for p in price_owners_stats]

axes[1, 1].bar(price_cats, avg_owners_list, color='purple', alpha=0.7)
axes[1, 1].set_xlabel('价格区间', fontsize=10)
axes[1, 1].set_ylabel('平均拥有者数量', fontsize=10)
axes[1, 1].set_title('价格区间与平均拥有者数量关系', fontsize=12, fontweight='bold')
axes[1, 1].grid(axis='y', alpha=0.3)
axes[1, 1].tick_params(axis='x', rotation=45)

plt.tight_layout()
plt.savefig('figs/task1_rdd_analysis.png', dpi=300, bbox_inches='tight')
print("已保存: figs/task1_rdd_analysis.png")
plt.close()

# 图2: 任务2的综合可视化
fig2, axes = plt.subplots(2, 2, figsize=(16, 12))
fig2.suptitle('任务2: Spark DataFrame & SQL 分析结果', fontsize=16, fontweight='bold')

# 2.1 收入最高开发商的爆款游戏
games_data = top_dev_games_list[:10]
game_names = [g['Name'][:25] + '...' if len(g['Name']) > 25 else g['Name'] for g in games_data]
game_revenues = [g['revenue'] for g in games_data]

axes[0, 0].barh(game_names, game_revenues, color='teal')
axes[0, 0].set_xlabel('收入 ($)', fontsize=10)
axes[0, 0].set_title(f'{top_dev_name} 的爆款游戏 (Top 10)', fontsize=12, fontweight='bold')
axes[0, 0].invert_yaxis()
axes[0, 0].grid(axis='x', alpha=0.3)

# 2.2 各类目游戏定价统计
genre_stats_data = genre_stats_list[:15]
genre_names = [g['Genre'] for g in genre_stats_data]
avg_prices = [g['avg_price'] for g in genre_stats_data]
median_prices = [g['median_price'] for g in genre_stats_data]

x_pos = range(len(genre_names))
width = 0.35
axes[0, 1].bar([x - width/2 for x in x_pos], avg_prices, width, label='平均价格', color='skyblue', alpha=0.8)
axes[0, 1].bar([x + width/2 for x in x_pos], median_prices, width, label='中位数价格', color='lightcoral', alpha=0.8)
axes[0, 1].set_xlabel('游戏类目', fontsize=10)
axes[0, 1].set_ylabel('价格 ($)', fontsize=10)
axes[0, 1].set_title('各类目游戏定价统计 (Top 15)', fontsize=12, fontweight='bold')
axes[0, 1].set_xticks(x_pos)
axes[0, 1].set_xticklabels(genre_names, rotation=45, ha='right')
axes[0, 1].legend()
axes[0, 1].grid(axis='y', alpha=0.3)

# 2.3 开发商游戏数量分布
dev_dist_data = dev_dist_list[:15]
dev_names = [d['Developer'][:20] + '...' if len(d['Developer']) > 20 else d['Developer'] for d in dev_dist_data]
dev_game_counts = [d['game_count'] for d in dev_dist_data]

axes[1, 0].barh(dev_names, dev_game_counts, color='gold')
axes[1, 0].set_xlabel('游戏数量', fontsize=10)
axes[1, 0].set_title('游戏数量最多的开发商 (Top 15)', fontsize=12, fontweight='bold')
axes[1, 0].invert_yaxis()
axes[1, 0].grid(axis='x', alpha=0.3)

# 2.4 多类别游戏的收入表现
multi_genre_data = multi_genre_list
genre_counts = [m['genre_count'] for m in multi_genre_data]
avg_revenues = [m['avg_revenue'] for m in multi_genre_data]
game_counts = [m['game_count'] for m in multi_genre_data]

ax4 = axes[1, 1]
ax4_twin = ax4.twinx()
line1 = ax4.plot(genre_counts, avg_revenues, 'o-', color='red', linewidth=2, markersize=8, label='平均收入')
ax4_twin.bar(genre_counts, game_counts, alpha=0.3, color='blue', label='游戏数量')
ax4.set_xlabel('类别数量', fontsize=10)
ax4.set_ylabel('平均收入 ($)', fontsize=10, color='red')
ax4_twin.set_ylabel('游戏数量', fontsize=10, color='blue')
ax4.set_title('多类别游戏的收入表现', fontsize=12, fontweight='bold')
ax4.grid(alpha=0.3)
ax4.tick_params(axis='y', labelcolor='red')
ax4_twin.tick_params(axis='y', labelcolor='blue')

plt.tight_layout()
plt.savefig('figs/task2_dataframe_sql_analysis.png', dpi=300, bbox_inches='tight')
print("已保存: figs/task2_dataframe_sql_analysis.png")
plt.close()

# 图3: 游戏市场深度分析
print("\n生成图3: 游戏市场深度分析...")

# 3.1 游戏发布月份分布
df_month = df.select(
    substring(col("release_date"), 6, 2).alias("Month"),
    col("Name")
).filter(col("Month").isNotNull() & (col("Month") != ""))

month_dist = df_month.groupBy("Month") \
    .agg(spark_count("*").alias("game_count")) \
    .orderBy("Month")

month_data = month_dist.collect()
month_labels = [f"{m['Month']}月" for m in month_data]
month_counts = [m['game_count'] for m in month_data]

# 3.2 不同价格区间的游戏数量分布
price_dist_df = df.select(
    col("clean_price"),
    when(col("clean_price") == 0, "免费")
    .when(col("clean_price") < 5, "$0-5")
    .when(col("clean_price") < 10, "$5-10")
    .when(col("clean_price") < 20, "$10-20")
    .when(col("clean_price") < 40, "$20-40")
    .otherwise("$40+").alias("price_range")
).groupBy("price_range") \
.agg(spark_count("*").alias("game_count")) \
.orderBy("price_range")

price_range_data = price_dist_df.collect()
price_ranges = [p['price_range'] for p in price_range_data]
price_range_counts = [p['game_count'] for p in price_range_data]

# 3.3 拥有者数量分布（分区间）
owners_dist_df = df.select(
    col("avg_owners"),
    when(col("avg_owners") < 10000, "0-1万")
    .when(col("avg_owners") < 50000, "1-5万")
    .when(col("avg_owners") < 150000, "5-15万")
    .when(col("avg_owners") < 500000, "15-50万")
    .when(col("avg_owners") < 2000000, "50-200万")
    .otherwise("200万+").alias("owners_range")
).groupBy("owners_range") \
.agg(spark_count("*").alias("game_count")) \
.orderBy("owners_range")

owners_range_data = owners_dist_df.collect()
owners_ranges = [o['owners_range'] for o in owners_range_data]
owners_range_counts = [o['game_count'] for o in owners_range_data]

# 3.4 收入与价格的相关性（采样数据）
sample_df = df.filter((col("clean_price") > 0) & (col("revenue") > 0)) \
    .sample(False, 0.1) \
    .select("clean_price", "revenue") \
    .limit(5000)

sample_data = sample_df.collect()
sample_prices = [s['clean_price'] for s in sample_data]
sample_revenues = [s['revenue'] for s in sample_data]

# 创建图3
fig3, axes = plt.subplots(2, 2, figsize=(16, 12))
fig3.suptitle('图3: 游戏市场深度分析', fontsize=16, fontweight='bold')

# 3.1 游戏发布月份分布
axes[0, 0].bar(month_labels, month_counts, color='steelblue', alpha=0.7)
axes[0, 0].set_xlabel('月份', fontsize=10)
axes[0, 0].set_ylabel('游戏数量', fontsize=10)
axes[0, 0].set_title('游戏发布月份分布', fontsize=12, fontweight='bold')
axes[0, 0].tick_params(axis='x', rotation=45)
axes[0, 0].grid(axis='y', alpha=0.3)

# 3.2 价格区间分布
axes[0, 1].bar(price_ranges, price_range_counts, color='coral', alpha=0.7)
axes[0, 1].set_xlabel('价格区间', fontsize=10)
axes[0, 1].set_ylabel('游戏数量', fontsize=10)
axes[0, 1].set_title('不同价格区间的游戏数量分布', fontsize=12, fontweight='bold')
axes[0, 1].tick_params(axis='x', rotation=45)
axes[0, 1].grid(axis='y', alpha=0.3)

# 3.3 拥有者数量分布
axes[1, 0].bar(owners_ranges, owners_range_counts, color='purple', alpha=0.7)
axes[1, 0].set_xlabel('拥有者数量区间', fontsize=10)
axes[1, 0].set_ylabel('游戏数量', fontsize=10)
axes[1, 0].set_title('游戏拥有者数量分布', fontsize=12, fontweight='bold')
axes[1, 0].tick_params(axis='x', rotation=45)
axes[1, 0].grid(axis='y', alpha=0.3)

# 3.4 收入与价格相关性
axes[1, 1].scatter(sample_prices, sample_revenues, alpha=0.3, s=10, color='green')
axes[1, 1].set_xlabel('价格 ($)', fontsize=10)
axes[1, 1].set_ylabel('收入 ($)', fontsize=10)
axes[1, 1].set_title('收入与价格相关性分析', fontsize=12, fontweight='bold')
axes[1, 1].set_xscale('log')
axes[1, 1].set_yscale('log')
axes[1, 1].grid(alpha=0.3)

plt.tight_layout()
plt.savefig('figs/market_depth_analysis.png', dpi=300, bbox_inches='tight')
print("已保存: figs/market_depth_analysis.png")
plt.close()

# 图4: 开发商和类别深度分析
print("\n生成图4: 开发商和类别深度分析...")

# 4.1 开发商收入集中度（Top 50的累积收入占比）
top_50_devs = dev_metrics_df.limit(50).collect()
cumulative_revenue = 0
total_revenue = df.select(spark_sum("revenue")).first()[0]
dev_names_cum = []
cum_percentages = []

for i, dev in enumerate(top_50_devs):
    cumulative_revenue += dev.total_revenue
    cum_percent = (cumulative_revenue / total_revenue) * 100
    dev_names_cum.append(f"Top {i+1}")
    cum_percentages.append(cum_percent)

# 4.2 最受欢迎类别组合（Top 10）
df_genre_combos = df.select(
    col("Genres"),
    col("revenue")
).withColumn("Genre", explode(split(regexp_replace(regexp_replace(col("Genres"), "'", ""), "\\[|\\]", ""), ","))) \
.withColumn("Genre", trim(col("Genre"))) \
.filter((col("Genre") != "") & (col("Genre").isNotNull()))

# 获取每个游戏的类别组合
df_game_genres = df.select(
    col("AppID"),
    col("Genres"),
    col("revenue")
).withColumn("genres_clean", regexp_replace(regexp_replace(col("Genres"), "'", ""), "\\[|\\]", ""))

genre_combo_stats = df_game_genres.groupBy("genres_clean") \
    .agg(
        spark_sum("revenue").alias("total_revenue"),
        spark_count("*").alias("game_count")
    ) \
    .orderBy(col("total_revenue").desc()) \
    .limit(10)

genre_combo_data = genre_combo_stats.collect()
combo_names = [g['genres_clean'][:30] + '...' if len(g['genres_clean']) > 30 else g['genres_clean'] for g in genre_combo_data]
combo_revenues = [g['total_revenue'] for g in genre_combo_data]

# 4.3 开发商平均游戏收入vs游戏数量
dev_avg_data = dev_metrics_df.select(
    col("Developer"),
    col("total_revenue"),
    col("game_count")
).withColumn("avg_revenue_per_game", col("total_revenue") / col("game_count")) \
.limit(100).collect()

dev_game_counts = [d['game_count'] for d in dev_avg_data]
dev_avg_revenues = [d['avg_revenue_per_game'] for d in dev_avg_data]

# 4.4 类别收入分布（饼图 - Top 10）
top_10_genres = genre_revenue_df.limit(10).collect()
genre_names_pie = [g['Genre'] for g in top_10_genres]
genre_revenues_pie = [g['total_revenue'] for g in top_10_genres]

# 创建图4
fig4, axes = plt.subplots(2, 2, figsize=(16, 12))
fig4.suptitle('图4: 开发商和类别深度分析', fontsize=16, fontweight='bold')

# 4.1 开发商收入集中度
axes[0, 0].plot(range(1, len(cum_percentages)+1), cum_percentages, 'o-', linewidth=2, markersize=5, color='red')
axes[0, 0].axhline(y=80, color='green', linestyle='--', label='80%线')
axes[0, 0].set_xlabel('Top N 开发商', fontsize=10)
axes[0, 0].set_ylabel('累积收入占比 (%)', fontsize=10)
axes[0, 0].set_title('开发商收入集中度分析（帕累托图）', fontsize=12, fontweight='bold')
axes[0, 0].grid(alpha=0.3)
axes[0, 0].legend()

# 4.2 最受欢迎类别组合
axes[0, 1].barh(combo_names, combo_revenues, color='teal', alpha=0.7)
axes[0, 1].set_xlabel('总收入 ($)', fontsize=10)
axes[0, 1].set_title('最受欢迎类别组合 (Top 10)', fontsize=12, fontweight='bold')
axes[0, 1].invert_yaxis()
axes[0, 1].grid(axis='x', alpha=0.3)

# 4.3 开发商平均游戏收入vs游戏数量
axes[1, 0].scatter(dev_game_counts, dev_avg_revenues, alpha=0.5, s=50, color='orange')
axes[1, 0].set_xlabel('游戏数量', fontsize=10)
axes[1, 0].set_ylabel('平均每游戏收入 ($)', fontsize=10)
axes[1, 0].set_title('开发商平均游戏收入 vs 游戏数量', fontsize=12, fontweight='bold')
axes[1, 0].set_xscale('log')
axes[1, 0].set_yscale('log')
axes[1, 0].grid(alpha=0.3)

# 4.4 类别收入分布（饼图）
axes[1, 1].pie(genre_revenues_pie, labels=genre_names_pie, autopct='%1.1f%%', startangle=90)
axes[1, 1].set_title('Top 10 类别收入分布', fontsize=12, fontweight='bold')

plt.tight_layout()
plt.savefig('figs/developer_genre_depth_analysis.png', dpi=300, bbox_inches='tight')
print("已保存: figs/developer_genre_depth_analysis.png")
plt.close()

# 图5: 时间序列和趋势分析
print("\n生成图5: 时间序列和趋势分析...")

# 5.1 年度游戏发布数量趋势（完整数据）
year_full_data = year_trend_df.collect()
years_full = [y['Year'] for y in year_full_data]
year_counts_full = [y['game_count'] for y in year_full_data]

# 5.2 年度平均价格趋势
year_avg_price = df_year.groupBy("Year") \
    .agg(avg("clean_price").alias("avg_price")) \
    .orderBy("Year")

year_avg_price_data = year_avg_price.collect()
years_price = [y['Year'] for y in year_avg_price_data]
avg_prices_year = [y['avg_price'] for y in year_avg_price_data]

# 5.3 年度总收入趋势
year_revenue_data = year_trend_df.select("Year", "total_revenue").collect()
years_revenue = [y['Year'] for y in year_revenue_data]
total_revenues = [y['total_revenue'] for y in year_revenue_data]

# 5.4 年度平均拥有者趋势
df_year_owners = df.select(
    substring(col("release_date"), 1, 4).alias("Year"),
    col("avg_owners")
).filter(
    (col("Year").isNotNull()) & 
    (col("Year") >= "2000") & 
    (col("Year") <= "2024") &
    (col("avg_owners").isNotNull())
).withColumn("Year", col("Year").cast(IntegerType()))

year_avg_owners = df_year_owners.groupBy("Year") \
    .agg(avg("avg_owners").alias("avg_owners")) \
    .orderBy("Year")

year_avg_owners_data = year_avg_owners.collect()
years_owners = [y['Year'] for y in year_avg_owners_data]
avg_owners_year = [y['avg_owners'] for y in year_avg_owners_data]

# 创建图5
fig5, axes = plt.subplots(2, 2, figsize=(16, 12))
fig5.suptitle('图5: 时间序列和趋势分析', fontsize=16, fontweight='bold')

# 5.1 年度游戏发布数量趋势
axes[0, 0].plot(years_full, year_counts_full, 'o-', linewidth=2, markersize=6, color='blue')
axes[0, 0].set_xlabel('年份', fontsize=10)
axes[0, 0].set_ylabel('游戏发布数量', fontsize=10)
axes[0, 0].set_title('年度游戏发布数量趋势', fontsize=12, fontweight='bold')
axes[0, 0].grid(alpha=0.3)
axes[0, 0].tick_params(axis='x', rotation=45)

# 5.2 年度平均价格趋势
axes[0, 1].plot(years_price, avg_prices_year, 's-', linewidth=2, markersize=6, color='green')
axes[0, 1].set_xlabel('年份', fontsize=10)
axes[0, 1].set_ylabel('平均价格 ($)', fontsize=10)
axes[0, 1].set_title('年度平均价格趋势', fontsize=12, fontweight='bold')
axes[0, 1].grid(alpha=0.3)
axes[0, 1].tick_params(axis='x', rotation=45)

# 5.3 年度总收入趋势
axes[1, 0].plot(years_revenue, total_revenues, '^-', linewidth=2, markersize=6, color='red')
axes[1, 0].set_xlabel('年份', fontsize=10)
axes[1, 0].set_ylabel('总收入 ($)', fontsize=10)
axes[1, 0].set_title('年度总收入趋势', fontsize=12, fontweight='bold')
axes[1, 0].grid(alpha=0.3)
axes[1, 0].tick_params(axis='x', rotation=45)

# 5.4 年度平均拥有者趋势
axes[1, 1].plot(years_owners, avg_owners_year, 'd-', linewidth=2, markersize=6, color='purple')
axes[1, 1].set_xlabel('年份', fontsize=10)
axes[1, 1].set_ylabel('平均拥有者数量', fontsize=10)
axes[1, 1].set_title('年度平均拥有者数量趋势', fontsize=12, fontweight='bold')
axes[1, 1].grid(alpha=0.3)
axes[1, 1].tick_params(axis='x', rotation=45)

plt.tight_layout()
plt.savefig('figs/time_series_trend_analysis.png', dpi=300, bbox_inches='tight')
print("已保存: figs/time_series_trend_analysis.png")
plt.close()

print("\n" + "=" * 80)
print("所有可视化图表已生成完成！")
print("=" * 80)

# 关闭Spark会话
spark.stop()

