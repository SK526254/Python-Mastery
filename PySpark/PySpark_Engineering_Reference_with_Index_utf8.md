# PySpark Engineering Reference

> **Purpose:** A production-oriented PySpark syntax and concepts reference for Data Engineering work and interviews.
>
> **Style:** Example → Concept. Searchable, concise, copy-paste friendly.
>
> **Version guidance:** Prefer modern PySpark DataFrame/Spark SQL APIs. Legacy APIs are included only where they help understand existing code.

---

## 00. Master Imports

### Core Spark

```python
from pyspark.sql import SparkSession
from pyspark import SparkContext
```

- `SparkSession`: main entry point for DataFrame/Spark SQL work.
- `SparkContext`: lower-level Spark context; mostly relevant for RDDs and legacy code.

### Functions

```python
from pyspark.sql import functions as F
from pyspark.sql.functions import (
    col, lit, when, expr,
    sum, avg, min, max, count, countDistinct,
    row_number, rank, dense_rank, lag, lead,
    to_date, to_timestamp, date_format, datediff,
    split, concat_ws, substring, regexp_replace,
    explode, array, array_contains,
    from_json, to_json, schema_of_json
)
```

**Preferred style:** `import functions as F` avoids name collisions with Python built-ins such as `sum`, `min`, and `max`.

### Window

```python
from pyspark.sql.window import Window
```

### Data types / schemas

```python
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, LongType,
    DoubleType, FloatType, BooleanType,
    DateType, TimestampType,
    ArrayType, MapType
)
```

### UDFs / Pandas UDFs

```python
from pyspark.sql.functions import udf, pandas_udf
```

### RDD broadcast / accumulators

```python
from pyspark import Broadcast
```

---

## Common Example Data

Assume these DataFrames are available throughout the reference:

```python
employees_df   # employee_id, name, department_id, salary, hire_date
departments_df # department_id, department
orders_df      # order_id, customer_id, order_date, amount, status
customers_df   # customer_id, customer_name, city
transactions_df # transaction_id, customer_id, amount, transaction_ts
```

Typical Spark session:

```python
spark = (
    SparkSession.builder
    .appName("PySparkReference")
    .getOrCreate()
)
```

---

<a id="ref-1-01-spark-fundamentals"></a>
## Index

> Click any item below to jump directly to that concept or section.

- [1. 01. Spark Fundamentals](#ref-1-01-spark-fundamentals)
    - [1.1. PySpark](#ref-1.1-pyspark)
    - [1.2. Features / Advantages](#ref-1.2-features-advantages)
    - [1.3. Modules & Packages](#ref-1.3-modules-packages)
    - [1.4. Cluster Managers](#ref-1.4-cluster-managers)
    - [1.5. SparkSession](#ref-1.5-sparksession)
    - [1.6. SparkContext](#ref-1.6-sparkcontext)
- [2. 02. Spark Architecture & Execution](#ref-2-02-spark-architecture-execution)
    - [2.1. Application](#ref-2.1-application)
    - [2.2. Driver](#ref-2.2-driver)
    - [2.3. Executor](#ref-2.3-executor)
    - [2.4. Job](#ref-2.4-job)
    - [2.5. Stage](#ref-2.5-stage)
    - [2.6. Task](#ref-2.6-task)
    - [2.7. Partition](#ref-2.7-partition)
    - [2.8. DAG](#ref-2.8-dag)
    - [2.9. Lazy Evaluation](#ref-2.9-lazy-evaluation)
    - [2.10. Transformation vs Action](#ref-2.10-transformation-vs-action)
    - [2.11. Transformations](#ref-2.11-transformations)
    - [2.12. Actions](#ref-2.12-actions)
    - [2.13. Narrow vs Wide Transformations](#ref-2.13-narrow-vs-wide-transformations)
    - [2.14. Narrow](#ref-2.14-narrow)
    - [2.15. Wide](#ref-2.15-wide)
- [3. 03. RDD](#ref-3-03-rdd)
    - [3.1. RDD](#ref-3.1-rdd)
    - [3.2. `parallelize()`](#ref-3.2-parallelize)
    - [3.3. `map()`](#ref-3.3-map)
    - [3.4. `flatMap()`](#ref-3.4-flatmap)
    - [3.5. `filter()`](#ref-3.5-filter)
    - [3.6. `foreach()`](#ref-3.6-foreach)
    - [3.7. RDD `collect()`](#ref-3.7-rdd-collect)
    - [3.8. RDD `repartition()` / `coalesce()`](#ref-3.8-rdd-repartition-coalesce)
    - [3.9. RDD → DataFrame](#ref-3.9-rdd-dataframe)
- [4. 04. DataFrame Fundamentals](#ref-4-04-dataframe-fundamentals)
    - [4.1. Create DataFrame](#ref-4.1-create-dataframe)
    - [4.2. Empty DataFrame](#ref-4.2-empty-dataframe)
    - [4.3. `StructType` / `StructField`](#ref-4.3-structtype-structfield)
    - [4.4. `show()`](#ref-4.4-show)
    - [4.5. `printSchema()`](#ref-4.5-printschema)
    - [4.6. `schema`](#ref-4.6-schema)
    - [4.7. `columns`](#ref-4.7-columns)
    - [4.8. DataFrame → Pandas](#ref-4.8-dataframe-pandas)
    - [4.9. `select()`](#ref-4.9-select)
    - [4.10. `selectExpr()`](#ref-4.10-selectexpr)
    - [4.11. `col()`](#ref-4.11-col)
    - [4.12. `lit()`](#ref-4.12-lit)
    - [4.13. `withColumn()`](#ref-4.13-withcolumn)
    - [4.14. `withColumnRenamed()`](#ref-4.14-withcolumnrenamed)
    - [4.15. `drop()`](#ref-4.15-drop)
    - [4.16. `dropDuplicates()`](#ref-4.16-dropduplicates)
    - [4.17. `distinct()`](#ref-4.17-distinct)
    - [4.18. `filter()` / `where()`](#ref-4.18-filter-where)
    - [4.19. `orderBy()` / `sort()`](#ref-4.19-orderby-sort)
    - [4.20. `sample()`](#ref-4.20-sample)
    - [4.21. `sampleBy()`](#ref-4.21-sampleby)
    - [4.22. `replace()`](#ref-4.22-replace)
    - [4.23. `fillna()`](#ref-4.23-fillna)
    - [4.24. `pivot()`](#ref-4.24-pivot)
    - [4.25. `transform()`](#ref-4.25-transform)
- [5. 05. DataFrame Operations](#ref-5-05-dataframe-operations)
    - [5.1. `groupBy()`](#ref-5.1-groupby)
    - [5.2. `agg()`](#ref-5.2-agg)
    - [5.3. `union()`](#ref-5.3-union)
    - [5.4. `unionByName()`](#ref-5.4-unionbyname)
    - [5.5. `unionAll()`](#ref-5.5-unionall)
    - [5.6. `map()` / `flatMap()` / `foreach()` on DataFrames](#ref-5.6-map-flatmap-foreach-on-dataframes)
- [6. 06. Column & Expression Functions](#ref-6-06-column-expression-functions)
    - [6.1. `expr()`](#ref-6.1-expr)
    - [6.2. `when()` / `otherwise()`](#ref-6.2-when-otherwise)
    - [6.3. `typedLit()`](#ref-6.3-typedlit)
- [7. 07. Filtering & Conditional Logic](#ref-7-07-filtering-conditional-logic)
    - [7.1. Null checks: `isNull()` / `isNotNull()`](#ref-7.1-null-checks-isnull-isnotnull)
    - [7.2. Null-safe equality: `<=>`](#ref-7.2-null-safe-equality)
    - [7.3. `coalesce()`](#ref-7.3-coalesce)
- [8. 08. Aggregations](#ref-8-08-aggregations)
    - [8.1. `count()`](#ref-8.1-count)
    - [8.2. `countDistinct()`](#ref-8.2-countdistinct)
    - [8.3. `sum()`](#ref-8.3-sum)
    - [8.4. `avg()`](#ref-8.4-avg)
    - [8.5. `min()` / `max()`](#ref-8.5-min-max)
    - [8.6. `first()` / `last()`](#ref-8.6-first-last)
    - [8.7. `collect_list()` / `collect_set()`](#ref-8.7-collect_list-collect_set)
    - [8.8. Conditional aggregation](#ref-8.8-conditional-aggregation)
- [9. 09. Joins](#ref-9-09-joins)
    - [9.1. Basic `join()`](#ref-9.1-basic-join)
    - [9.2. Join Types](#ref-9.2-join-types)
    - [9.3. Inner](#ref-9.3-inner)
    - [9.4. Left](#ref-9.4-left)
    - [9.5. Right](#ref-9.5-right)
    - [9.6. Full / Full Outer](#ref-9.6-full-full-outer)
    - [9.7. Left Semi](#ref-9.7-left-semi)
    - [9.8. Left Anti](#ref-9.8-left-anti)
    - [9.9. Cross](#ref-9.9-cross)
    - [9.10. Multiple-column join](#ref-9.10-multiple-column-join)
    - [9.11. Join expression + aliases](#ref-9.11-join-expression-aliases)
    - [9.12. Duplicate join columns](#ref-9.12-duplicate-join-columns)
    - [9.13. Broadcast join](#ref-9.13-broadcast-join)
- [10. 10. Window Functions](#ref-10-10-window-functions)
    - [10.1. `Window`](#ref-10.1-window)
    - [10.2. `partitionBy()`](#ref-10.2-partitionby)
    - [10.3. `orderBy()`](#ref-10.3-orderby)
    - [10.4. `rowsBetween()`](#ref-10.4-rowsbetween)
    - [10.5. `rangeBetween()`](#ref-10.5-rangebetween)
    - [10.6. `row_number()`](#ref-10.6-row_number)
    - [10.7. `rank()`](#ref-10.7-rank)
    - [10.8. `dense_rank()`](#ref-10.8-dense_rank)
    - [10.9. `percent_rank()`](#ref-10.9-percent_rank)
    - [10.10. `lag()` / `lead()`](#ref-10.10-lag-lead)
    - [10.11. Window `first()` / `last()`](#ref-10.11-window-first-last)
    - [10.12. Running aggregate](#ref-10.12-running-aggregate)
    - [10.13. GroupBy vs Window](#ref-10.13-groupby-vs-window)
- [11. 11. String Functions](#ref-11-11-string-functions)
    - [11.1. `split()`](#ref-11.1-split)
    - [11.2. `concat_ws()`](#ref-11.2-concat_ws)
    - [11.3. `substring()`](#ref-11.3-substring)
    - [11.4. `translate()`](#ref-11.4-translate)
    - [11.5. `regexp_replace()`](#ref-11.5-regexp_replace)
    - [11.6. `overlay()`](#ref-11.6-overlay)
    - [11.7. Other high-value string functions](#ref-11.7-other-high-value-string-functions)
- [12. 12. Date & Timestamp Functions](#ref-12-12-date-timestamp-functions)
    - [12.1. `to_date()`](#ref-12.1-to_date)
    - [12.2. `to_timestamp()`](#ref-12.2-to_timestamp)
    - [12.3. `date_format()`](#ref-12.3-date_format)
    - [12.4. `datediff()`](#ref-12.4-datediff)
    - [12.5. `months_between()`](#ref-12.5-months_between)
    - [12.6. Extractors](#ref-12.6-extractors)
    - [12.7. `date_add()` / `date_sub()`](#ref-12.7-date_add-date_sub)
    - [12.8. Current date/time](#ref-12.8-current-date-time)
- [13. 13. Array Functions](#ref-13-13-array-functions)
    - [13.1. `array()`](#ref-13.1-array)
    - [13.2. `explode()`](#ref-13.2-explode)
    - [13.3. `array_contains()`](#ref-13.3-array_contains)
    - [13.4. `collect_list()` / `collect_set()`](#ref-13.4-collect_list-collect_set)
    - [13.5. Other high-value array functions](#ref-13.5-other-high-value-array-functions)
- [14. 14. Map Functions](#ref-14-14-map-functions)
    - [14.1. `MapType`](#ref-14.1-maptype)
    - [14.2. `create_map()`](#ref-14.2-create_map)
    - [14.3. `map_keys()` / `map_values()`](#ref-14.3-map_keys-map_values)
    - [14.4. Other useful map operations](#ref-14.4-other-useful-map-operations)
- [15. 15. Struct & Nested Data](#ref-15-15-struct-nested-data)
    - [15.1. `struct()`](#ref-15.1-struct)
    - [15.2. Nested field access](#ref-15.2-nested-field-access)
    - [15.3. `StructType` / `StructField`](#ref-15.3-structtype-structfield)
- [16. 16. JSON Functions](#ref-16-16-json-functions)
    - [16.1. `from_json()`](#ref-16.1-from_json)
    - [16.2. `to_json()`](#ref-16.2-to_json)
    - [16.3. `schema_of_json()`](#ref-16.3-schema_of_json)
    - [16.4. `json_tuple()`](#ref-16.4-json_tuple)
    - [16.5. `get_json_object()`](#ref-16.5-get_json_object)
- [17. 17. Null Handling](#ref-17-17-null-handling)
    - [17.1. `isNull()` / `isNotNull()`](#ref-17.1-isnull-isnotnull)
    - [17.2. `fillna()`](#ref-17.2-fillna)
    - [17.3. `coalesce()` function](#ref-17.3-coalesce-function)
    - [17.4. `when()` / `otherwise()`](#ref-17.4-when-otherwise)
    - [17.5. Null-safe comparison](#ref-17.5-null-safe-comparison)
- [18. 18. UDFs](#ref-18-18-udfs)
    - [18.1. Python UDF](#ref-18.1-python-udf)
    - [18.2. Built-in function vs UDF](#ref-18.2-built-in-function-vs-udf)
    - [18.3. Pandas UDF](#ref-18.3-pandas-udf)
    - [18.4. `applyInPandas()`](#ref-18.4-applyinpandas)
- [19. 19. Data Sources](#ref-19-19-data-sources)
    - [19.1. General read pattern](#ref-19.1-general-read-pattern)
    - [19.2. General write pattern](#ref-19.2-general-write-pattern)
    - [19.3. CSV](#ref-19.3-csv)
    - [19.4. Parquet](#ref-19.4-parquet)
    - [19.5. JSON](#ref-19.5-json)
    - [19.6. Hive](#ref-19.6-hive)
    - [19.7. JDBC](#ref-19.7-jdbc)
    - [19.8. Query](#ref-19.8-query)
    - [19.9. Parallel JDBC read](#ref-19.9-parallel-jdbc-read)
    - [19.10. SQL Server](#ref-19.10-sql-server)
    - [19.11. MySQL](#ref-19.11-mysql)
    - [19.12. Partitioned write: `partitionBy()`](#ref-19.12-partitioned-write-partitionby)
- [20. 20. Partitioning](#ref-20-20-partitioning)
    - [20.1. Partition concept](#ref-20.1-partition-concept)
    - [20.2. `repartition()`](#ref-20.2-repartition)
    - [20.3. `coalesce()`](#ref-20.3-coalesce)
    - [20.4. `partitionBy()`](#ref-20.4-partitionby)
    - [20.5. Hash partitioning](#ref-20.5-hash-partitioning)
    - [20.6. Partition pruning](#ref-20.6-partition-pruning)
- [21. 21. Performance Optimization](#ref-21-21-performance-optimization)
    - [21.1. Optimization decision order](#ref-21.1-optimization-decision-order)
    - [21.2. `explain()`](#ref-21.2-explain)
    - [21.3. Predicate pushdown](#ref-21.3-predicate-pushdown)
    - [21.4. Column pruning](#ref-21.4-column-pruning)
    - [21.5. Broadcast join](#ref-21.5-broadcast-join)
    - [21.6. Shuffle](#ref-21.6-shuffle)
    - [21.7. Data skew](#ref-21.7-data-skew)
    - [21.8. Adaptive Query Execution (AQE)](#ref-21.8-adaptive-query-execution-aqe)
    - [21.9. Cache / `cache()`](#ref-21.9-cache-cache)
    - [21.10. `persist()`](#ref-21.10-persist)
    - [21.11. `unpersist()`](#ref-21.11-unpersist)
    - [21.12. File sizes / small-file problem](#ref-21.12-file-sizes-small-file-problem)
    - [21.13. Driver memory](#ref-21.13-driver-memory)
    - [21.14. Built-ins vs UDF](#ref-21.14-built-ins-vs-udf)
    - [21.15. Join optimization](#ref-21.15-join-optimization)
    - [21.16. Partition count](#ref-21.16-partition-count)
- [22. 22. Spark UI & Debugging](#ref-22-22-spark-ui-debugging)
    - [22.1. Jobs tab](#ref-22.1-jobs-tab)
    - [22.2. Stages tab](#ref-22.2-stages-tab)
    - [22.3. Tasks](#ref-22.3-tasks)
    - [22.4. SQL tab](#ref-22.4-sql-tab)
    - [22.5. Executors tab](#ref-22.5-executors-tab)
    - [22.6. Storage tab](#ref-22.6-storage-tab)
    - [22.7. Slow-job checklist](#ref-22.7-slow-job-checklist)
- [23. 23. Data Engineering Patterns](#ref-23-23-data-engineering-patterns)
    - [23.1. Duplicate detection](#ref-23.1-duplicate-detection)
    - [23.2. Null validation](#ref-23.2-null-validation)
    - [23.3. Referential integrity](#ref-23.3-referential-integrity)
    - [23.4. Record-count validation](#ref-23.4-record-count-validation)
    - [23.5. Invalid-date detection](#ref-23.5-invalid-date-detection)
    - [23.6. Schema validation](#ref-23.6-schema-validation)
    - [23.7. Incremental filtering](#ref-23.7-incremental-filtering)
    - [23.8. Latest record per key](#ref-23.8-latest-record-per-key)
    - [23.9. SCD Type 1 — conceptual pattern](#ref-23.9-scd-type-1-conceptual-pattern)
    - [23.10. SCD Type 2 — conceptual pattern](#ref-23.10-scd-type-2-conceptual-pattern)
    - [23.11. Idempotent transformation](#ref-23.11-idempotent-transformation)
- [24. 24. Recommended Missing Topics](#ref-24-24-recommended-missing-topics)
    - [24.1. 24.1 Spark SQL](#ref-24.1-24-1-spark-sql)
    - [24.2. 24.2 Catalyst Optimizer](#ref-24.2-24-2-catalyst-optimizer)
    - [24.3. 24.3 Tungsten / whole-stage code generation](#ref-24.3-24-3-tungsten-whole-stage-code-generation)
    - [24.4. 24.4 Adaptive Query Execution](#ref-24.4-24-4-adaptive-query-execution)
    - [24.5. 24.5 `spark.sql.shuffle.partitions`](#ref-24.5-24-5-spark-sql-shuffle-partitions)
    - [24.6. 24.6 Broadcast threshold](#ref-24.6-24-6-broadcast-threshold)
    - [24.7. 24.7 Checkpointing](#ref-24.7-24-7-checkpointing)
    - [24.8. 24.8 Broadcast variable vs broadcast join](#ref-24.8-24-8-broadcast-variable-vs-broadcast-join)
    - [24.9. Broadcast variable](#ref-24.9-broadcast-variable)
    - [24.10. Broadcast join](#ref-24.10-broadcast-join)
    - [24.11. 24.9 Accumulator](#ref-24.11-24-9-accumulator)
    - [24.12. 24.10 Structured Streaming](#ref-24.12-24-10-structured-streaming)
    - [24.13. 24.11 Watermarking](#ref-24.13-24-11-watermarking)
    - [24.14. 24.12 Schema evolution](#ref-24.14-24-12-schema-evolution)
    - [24.15. 24.13 Data contracts](#ref-24.15-24-13-data-contracts)
    - [24.16. 24.14 Observability](#ref-24.16-24-14-observability)
    - [24.17. 24.15 Testing](#ref-24.17-24-15-testing)
    - [24.18. 24.16 Secrets / credentials](#ref-24.18-24-16-secrets-credentials)
    - [24.19. 24.17 Data formats](#ref-24.19-24-17-data-formats)
    - [24.20. 24.18 Delta/Iceberg/Hudi-style table formats](#ref-24.20-24-18-delta-iceberg-hudi-style-table-formats)
- [25. Requested Topics Validation](#ref-25-requested-topics-validation)
- [26. 25. PySpark Quick Reference Cheat Sheet](#ref-26-25-pyspark-quick-reference-cheat-sheet)
    - [26.1. SparkSession](#ref-26.1-sparksession)
    - [26.2. DataFrame Creation](#ref-26.2-dataframe-creation)
    - [26.3. Inspection](#ref-26.3-inspection)
    - [26.4. Selection](#ref-26.4-selection)
    - [26.5. Filtering](#ref-26.5-filtering)
    - [26.6. Column Operations](#ref-26.6-column-operations)
    - [26.7. Aggregations](#ref-26.7-aggregations)
    - [26.8. Joins](#ref-26.8-joins)
    - [26.9. Windows](#ref-26.9-windows)
    - [26.10. Strings](#ref-26.10-strings)
    - [26.11. Dates](#ref-26.11-dates)
    - [26.12. Arrays](#ref-26.12-arrays)
    - [26.13. Maps](#ref-26.13-maps)
    - [26.14. Struct / Nested](#ref-26.14-struct-nested)
    - [26.15. JSON](#ref-26.15-json)
    - [26.16. Null Handling](#ref-26.16-null-handling)
    - [26.17. Partitioning](#ref-26.17-partitioning)
    - [26.18. Performance](#ref-26.18-performance)
    - [26.19. Read](#ref-26.19-read)
    - [26.20. Write](#ref-26.20-write)
    - [26.21. RDD](#ref-26.21-rdd)
    - [26.22. High-Value Interview Reminders](#ref-26.22-high-value-interview-reminders)
- [27. Practical Engineering Rules](#ref-27-practical-engineering-rules)

---
# 01. Spark Fundamentals

<a id="ref-1.1-pyspark"></a>
## PySpark

**Purpose:** Python API for Apache Spark, used for distributed data processing with DataFrames, SQL, RDDs, streaming, and ML.

**Remember:** Modern Data Engineering generally favors DataFrames/Spark SQL over direct RDD programming.

---

<a id="ref-1.2-features-advantages"></a>
## Features / Advantages

- Distributed processing across executors.
- Lazy evaluation and DAG-based execution.
- Built-in SQL/DataFrame optimizer.
- Fault tolerance through lineage.
- Supports batch, streaming, SQL, and ML workloads.
- Connects to files, databases, object stores, and warehouse systems.

**Production importance:** ⭐⭐⭐⭐⭐

---

<a id="ref-1.3-modules-packages"></a>
## Modules & Packages

Common namespaces:

```python
pyspark.sql
pyspark.sql.functions
pyspark.sql.window
pyspark.sql.types
pyspark
```

---

<a id="ref-1.4-cluster-managers"></a>
## Cluster Managers

Spark can run with:

- Standalone
- YARN
- Kubernetes
- Managed/cloud Spark platforms

**Remember:** The cluster manager allocates resources; Spark's driver/executors execute the application.

---

<a id="ref-1.5-sparksession"></a>
## SparkSession

**Purpose:** Entry point for DataFrame and Spark SQL operations.

**Import:**

```python
from pyspark.sql import SparkSession
```

**Syntax:**

```python
spark = SparkSession.builder.appName("app").getOrCreate()
```

**Example:**

```python
spark = SparkSession.builder.appName("OrdersETL").getOrCreate()
orders_df = spark.read.parquet("/data/orders")
```

**Remember:** Prefer `SparkSession` for modern applications instead of manually creating `SQLContext`.

**Production:** ⭐⭐⭐⭐⭐

---

<a id="ref-1.6-sparkcontext"></a>
## SparkContext

**Purpose:** Low-level connection to the Spark cluster; mainly relevant to RDD APIs and some runtime features.

**Import:**

```python
from pyspark import SparkContext
```

**Syntax:**

```python
sc = SparkContext.getOrCreate()
```

**Example:**

```python
rdd = sc.parallelize([1, 2, 3, 4])
```

**Remember:** DataFrame applications normally access it through `spark.sparkContext`.

---

<a id="ref-2-02-spark-architecture-execution"></a>
# 02. Spark Architecture & Execution

<a id="ref-2.1-application"></a>
## Application

One submitted Spark program consisting of a driver and its executors.

<a id="ref-2.2-driver"></a>
## Driver

Runs application logic, builds execution plans, coordinates jobs, and schedules tasks.

<a id="ref-2.3-executor"></a>
## Executor

Worker process that executes tasks and stores cached data.

<a id="ref-2.4-job"></a>
## Job

Usually created by an action such as `count()` or `write`.

<a id="ref-2.5-stage"></a>
## Stage

A group of tasks that can execute without crossing a shuffle boundary.

<a id="ref-2.6-task"></a>
## Task

Smallest unit of execution, normally processing one partition.

<a id="ref-2.7-partition"></a>
## Partition

A logical chunk of distributed data. Tasks operate on partitions.

<a id="ref-2.8-dag"></a>
## DAG

Directed Acyclic Graph representing the sequence of transformations Spark must execute.

---

<a id="ref-2.9-lazy-evaluation"></a>
## Lazy Evaluation

Transformations build a plan but do not execute immediately.

```python
filtered = orders_df.filter(F.col("amount") > 100)
result = filtered.groupBy("customer_id").agg(F.sum("amount"))

result.show()  # execution is triggered here
```

**Why it matters:** Spark can optimize the complete plan before execution.

---

<a id="ref-2.10-transformation-vs-action"></a>
## Transformation vs Action

<a id="ref-2.11-transformations"></a>
### Transformations

Return a new DataFrame/RDD and are generally lazy.

```python
select()
filter()
where()
withColumn()
join()
groupBy()
repartition()
```

<a id="ref-2.12-actions"></a>
### Actions

Trigger execution and return results or write data.

```python
show()
count()
collect()
first()
take()
write...
```

**Pitfall:** Calling actions repeatedly can execute the same lineage repeatedly.

---

<a id="ref-2.13-narrow-vs-wide-transformations"></a>
## Narrow vs Wide Transformations

<a id="ref-2.14-narrow"></a>
### Narrow

Each output partition depends on a small number of input partitions.

```python
df.filter(...)
df.select(...)
df.withColumn(...)
```

Usually no shuffle.

<a id="ref-2.15-wide"></a>
### Wide

Output partitions depend on multiple input partitions.

```python
df.groupBy(...)
df.join(...)
df.repartition(...)
```

Usually introduces shuffle.

**Interview:** ⭐⭐⭐⭐⭐  
**Production:** ⭐⭐⭐⭐⭐

---

<a id="ref-3-03-rdd"></a>
# 03. RDD

<a id="ref-3.1-rdd"></a>
## RDD

**Purpose:** Fault-tolerant distributed collection of objects.

**Import:**

```python
from pyspark import SparkContext
```

**Example:**

```python
sc = spark.sparkContext
rdd = sc.parallelize([1, 2, 3, 4])
```

**Remember:** RDDs give lower-level control but generally lose DataFrame/Spark SQL optimizer advantages.

---

<a id="ref-3.2-parallelize"></a>
## `parallelize()`

**Purpose:** Creates an RDD from local Python data.

**Syntax:**

```python
sc.parallelize(collection, numSlices=None)
```

**Example:**

```python
rdd = spark.sparkContext.parallelize([10, 20, 30], 2)
```

---

<a id="ref-3.3-map"></a>
## `map()`

**Purpose:** Applies a function to every RDD element.

**Example:**

```python
rdd.map(lambda x: x * 2).collect()
```

---

<a id="ref-3.4-flatmap"></a>
## `flatMap()`

**Purpose:** Applies a function and flattens the returned iterables.

**Example:**

```python
words = spark.sparkContext.parallelize(["data engineering", "spark"])
words.flatMap(lambda x: x.split()).collect()
```

---

<a id="ref-3.5-filter"></a>
## `filter()`

**Purpose:** Keeps elements matching a condition.

**Example:**

```python
rdd.filter(lambda x: x > 20).collect()
```

---

<a id="ref-3.6-foreach"></a>
## `foreach()`

**Purpose:** Executes a function for each RDD element, typically for side effects.

**Example:**

```python
rdd.foreach(lambda x: print(x))
```

**Pitfall:** Output from executor-side `print()` is not the same as driver output.

---

<a id="ref-3.7-rdd-collect"></a>
## RDD `collect()`

**Purpose:** Brings all RDD elements to the driver.

**Example:**

```python
rdd.collect()
```

**Pitfall:** Can cause driver OOM for large datasets.

---

<a id="ref-3.8-rdd-repartition-coalesce"></a>
## RDD `repartition()` / `coalesce()`

```python
rdd.repartition(20)
rdd.coalesce(5)
```

Same core idea as DataFrames: repartition generally shuffles; coalesce is mainly useful for reducing partitions.

---

<a id="ref-3.9-rdd-dataframe"></a>
## RDD → DataFrame

```python
rdd = spark.sparkContext.parallelize([
    (1, "Sai", 90000),
    (2, "Ravi", 80000)
])

df = rdd.toDF(["id", "name", "salary"])
```

---

<a id="ref-4-04-dataframe-fundamentals"></a>
# 04. DataFrame Fundamentals

<a id="ref-4.1-create-dataframe"></a>
## Create DataFrame

**Import:**

```python
from pyspark.sql import SparkSession
```

**Example:**

```python
data = [(1, "Sai"), (2, "Ravi")]
df = spark.createDataFrame(data, ["id", "name"])
```

---

<a id="ref-4.2-empty-dataframe"></a>
## Empty DataFrame

**Import:**

```python
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
```

**Example:**

```python
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True)
])

empty_df = spark.createDataFrame([], schema)
```

**Remember:** Define a schema explicitly when an empty dataset cannot provide type inference.

---

<a id="ref-4.3-structtype-structfield"></a>
## `StructType` / `StructField`

**Purpose:** Explicitly define DataFrame schemas.

**Example:**

```python
schema = StructType([
    StructField("employee_id", IntegerType(), False),
    StructField("name", StringType(), True)
])
```

---

<a id="ref-4.4-show"></a>
## `show()`

**Purpose:** Displays rows for inspection.

**Example:**

```python
employees_df.show(10, truncate=False)
```

**Pitfall:** Debugging only; it is an action.

---

<a id="ref-4.5-printschema"></a>
## `printSchema()`

```python
employees_df.printSchema()
```

Shows nested and field data types.

---

<a id="ref-4.6-schema"></a>
## `schema`

```python
employees_df.schema
```

Returns the `StructType` schema object.

---

<a id="ref-4.7-columns"></a>
## `columns`

```python
employees_df.columns
```

Returns column names.

---

<a id="ref-4.8-dataframe-pandas"></a>
## DataFrame → Pandas

**Purpose:** Converts distributed Spark data to a local Pandas DataFrame.

**Example:**

```python
pdf = employees_df.limit(10000).toPandas()
```

**Pitfall:** Entire result is collected to the driver. Limit/sample first for large data.

---

<a id="ref-4.9-select"></a>
## `select()`

**Import:**

```python
from pyspark.sql import functions as F
```

**Syntax:**

```python
df.select("col1", "col2")
df.select(F.col("salary") * 12)
```

**Example:**

```python
employees_df.select("name", "salary")
```

**SQL:**

```sql
SELECT name, salary
FROM employees;
```

---

<a id="ref-4.10-selectexpr"></a>
## `selectExpr()`

**Purpose:** Selects columns using SQL expressions.

**Import:**

```python
from pyspark.sql import functions as F
```

**Example:**

```python
employees_df.selectExpr(
    "name",
    "salary * 12 AS annual_salary"
)
```

**Remember:** Useful when an expression is clearer in SQL syntax.

---

<a id="ref-4.11-col"></a>
## `col()`

**Import:**

```python
from pyspark.sql.functions import col
```

**Syntax:**

```python
col("column_name")
```

**Example:**

```python
employees_df.select(col("name"), col("salary"))
```

---

<a id="ref-4.12-lit"></a>
## `lit()`

**Purpose:** Creates a literal constant column/expression.

**Import:**

```python
from pyspark.sql.functions import lit
```

**Example:**

```python
employees_df.withColumn("source", lit("HR"))
```

---

<a id="ref-4.13-withcolumn"></a>
## `withColumn()`

**Purpose:** Adds or replaces a column.

**Import:**

```python
from pyspark.sql.functions import col
```

**Syntax:**

```python
df.withColumn("new_column", expression)
```

**Example:**

```python
orders_df.withColumn(
    "amount_with_tax",
    col("amount") * 1.18
)
```

**Remember:** DataFrames are immutable.

---

<a id="ref-4.14-withcolumnrenamed"></a>
## `withColumnRenamed()`

**Example:**

```python
employees_df.withColumnRenamed("salary", "base_salary")
```

---

<a id="ref-4.15-drop"></a>
## `drop()`

**Example:**

```python
employees_df.drop("temporary_column")
```

---

<a id="ref-4.16-dropduplicates"></a>
## `dropDuplicates()`

**Purpose:** Removes duplicate rows, optionally based on selected columns.

**Example:**

```python
customers_df.dropDuplicates(["customer_id"])
```

**Pitfall:** For "latest record per key", use a window rather than blindly dropping duplicates.

---

<a id="ref-4.17-distinct"></a>
## `distinct()`

```python
orders_df.select("customer_id").distinct()
```

Removes duplicate rows across the selected columns.

---

<a id="ref-4.18-filter-where"></a>
## `filter()` / `where()`

**Purpose:** Filters rows. `where()` is an alias-style alternative to `filter()`.

**Import:**

```python
from pyspark.sql.functions import col
```

**Example:**

```python
orders_df.filter(
    (col("amount") > 100) & (col("status") == "COMPLETE")
)
```

**SQL:**

```sql
SELECT *
FROM orders
WHERE amount > 100
  AND status = 'COMPLETE';
```

**Pitfall:** Use `&`, `|`, and `~` for Spark column boolean expressions; do not use Python `and`, `or`, `not`.

---

<a id="ref-4.19-orderby-sort"></a>
## `orderBy()` / `sort()`

```python
orders_df.orderBy(F.col("amount").desc())
orders_df.sort("order_date")
```

**Remember:** Global sorting can be expensive because it requires distributed ordering.

---

<a id="ref-4.20-sample"></a>
## `sample()`

**Example:**

```python
orders_df.sample(withReplacement=False, fraction=0.1, seed=42)
```

---

<a id="ref-4.21-sampleby"></a>
## `sampleBy()`

**Purpose:** Stratified sampling by a categorical column.

**Example:**

```python
orders_df.sampleBy(
    "status",
    fractions={"COMPLETE": 0.1, "CANCELLED": 0.2},
    seed=42
)
```

---

<a id="ref-4.22-replace"></a>
## `replace()`

**Purpose:** Replaces values in DataFrame columns.

**Example:**

```python
df.replace({"UNKNOWN": None}, subset=["city"])
```

---

<a id="ref-4.23-fillna"></a>
## `fillna()`

**Purpose:** Replaces nulls with specified values.

**Example:**

```python
orders_df.fillna({"status": "UNKNOWN", "amount": 0})
```

**Remember:** `fillna()` is the standard DataFrame API; `fill()` is commonly encountered as an equivalent-style operation in some contexts but is not a separate preferred DataFrame method to rely on.

---

<a id="ref-4.24-pivot"></a>
## `pivot()`

**Purpose:** Converts distinct row values into columns for aggregation.

**Example:**

```python
orders_df.groupBy("customer_id").pivot("status").agg(F.sum("amount"))
```

**Pitfall:** High-cardinality pivot columns can create very wide data and expensive jobs.

---

<a id="ref-4.25-transform"></a>
## `transform()`

**Purpose:** Applies a custom DataFrame-to-DataFrame function while keeping transformations chainable.

**Syntax:**

```python
df.transform(function)
```

**Example:**

```python
def add_tax(df):
    return df.withColumn("tax", F.col("amount") * 0.18)

result = orders_df.transform(add_tax)
```

**Production:** ⭐⭐⭐⭐

---

<a id="ref-5-05-dataframe-operations"></a>
# 05. DataFrame Operations

<a id="ref-5.1-groupby"></a>
## `groupBy()`

**Purpose:** Groups rows before aggregation.

**Example:**

```python
employees_df.groupBy("department_id").agg(
    F.avg("salary").alias("avg_salary")
)
```

**SQL:**

```sql
SELECT department_id, AVG(salary)
FROM employees
GROUP BY department_id;
```

**Remember:** `groupBy()` alone does not produce final results; aggregation/action completes the operation.

---

<a id="ref-5.2-agg"></a>
## `agg()`

**Example:**

```python
orders_df.groupBy("customer_id").agg(
    F.sum("amount").alias("total_amount"),
    F.count("*").alias("order_count")
)
```

---

<a id="ref-5.3-union"></a>
## `union()`

**Purpose:** Appends rows by column position.

**Example:**

```python
df1.union(df2)
```

**Pitfall:** Matching names is not enough; column order matters.

---

<a id="ref-5.4-unionbyname"></a>
## `unionByName()`

**Purpose:** Appends rows matching columns by name.

**Example:**

```python
df1.unionByName(df2)
```

Optional missing columns:

```python
df1.unionByName(df2, allowMissingColumns=True)
```

**Preferred:** Use `unionByName()` when schemas may differ in column order.

---

<a id="ref-5.5-unionall"></a>
## `unionAll()`

Historically an alias for `union()`.

```python
df1.unionAll(df2)
```

**Modern guidance:** Prefer `union()` or `unionByName()`.

---

<a id="ref-5.6-map-flatmap-foreach-on-dataframes"></a>
## `map()` / `flatMap()` / `foreach()` on DataFrames

These are **not normal DataFrame row-wise APIs** like their RDD counterparts.

For DataFrames, prefer:

- built-in column expressions
- `transform()`
- `mapInPandas()` / `applyInPandas()` where a supported vectorized pattern is genuinely required

**Interview pitfall:** Saying `df.map(...)` is standard DataFrame usage is incorrect.

---

<a id="ref-6-06-column-expression-functions"></a>
# 06. Column & Expression Functions

<a id="ref-6.1-expr"></a>
## `expr()`

**Purpose:** Builds a Spark SQL expression from a string.

**Import:**

```python
from pyspark.sql.functions import expr
```

**Example:**

```python
orders_df.select(
    expr("amount * 1.18 AS amount_with_tax")
)
```

---

<a id="ref-6.2-when-otherwise"></a>
## `when()` / `otherwise()`

**Purpose:** Builds conditional expressions.

**Import:**

```python
from pyspark.sql.functions import when, col
```

**Example:**

```python
orders_df.withColumn(
    "order_type",
    when(col("amount") >= 500, "HIGH")
    .when(col("amount") >= 100, "MEDIUM")
    .otherwise("LOW")
)
```

**SQL:**

```sql
CASE
  WHEN amount >= 500 THEN 'HIGH'
  WHEN amount >= 100 THEN 'MEDIUM'
  ELSE 'LOW'
END
```

---

<a id="ref-6.3-typedlit"></a>
## `typedLit()`

**Purpose:** Creates a literal with a Spark-supported complex type.

**Import:**

```python
from pyspark.sql.functions import typedLit
```

**Example:**

```python
df.withColumn("tags", typedLit(["etl", "spark"]))
```

**Remember:** Use when `lit()` cannot conveniently represent the required complex Python value/type.

---

<a id="ref-7-07-filtering-conditional-logic"></a>
# 07. Filtering & Conditional Logic

<a id="ref-7.1-null-checks-isnull-isnotnull"></a>
## Null checks: `isNull()` / `isNotNull()`

**Import:**

```python
from pyspark.sql.functions import col
```

**Example:**

```python
customers_df.filter(col("city").isNull())
customers_df.filter(col("city").isNotNull())
```

---

<a id="ref-7.2-null-safe-equality"></a>
## Null-safe equality: `<=>`

**Purpose:** Treats two null values as equal.

**Example:**

```python
df.filter(expr("left_value <=> right_value"))
```

**Remember:** Normal `=` does not match null to null.

---

<a id="ref-7.3-coalesce"></a>
## `coalesce()`

**Purpose:** Returns the first non-null expression.

**Import:**

```python
from pyspark.sql.functions import coalesce, col, lit
```

**Example:**

```python
df.withColumn(
    "final_city",
    coalesce(col("city"), col("backup_city"), lit("UNKNOWN"))
)
```

**Important:** This function is also relevant to partition/file-size optimization as a DataFrame method `df.coalesce(n)`; do not confuse the two.

---

<a id="ref-8-08-aggregations"></a>
# 08. Aggregations

<a id="ref-8.1-count"></a>
## `count()`

**Import:**

```python
from pyspark.sql import functions as F
```

**Examples:**

```python
orders_df.agg(F.count("*"))
orders_df.groupBy("customer_id").agg(F.count("*").alias("orders"))
```

---

<a id="ref-8.2-countdistinct"></a>
## `countDistinct()`

```python
orders_df.agg(
    F.countDistinct("customer_id").alias("unique_customers")
)
```

---

<a id="ref-8.3-sum"></a>
## `sum()`

```python
orders_df.groupBy("customer_id").agg(
    F.sum("amount").alias("total_amount")
)
```

---

<a id="ref-8.4-avg"></a>
## `avg()`

```python
employees_df.groupBy("department_id").agg(
    F.avg("salary").alias("avg_salary")
)
```

---

<a id="ref-8.5-min-max"></a>
## `min()` / `max()`

```python
orders_df.agg(
    F.min("amount").alias("min_amount"),
    F.max("amount").alias("max_amount")
)
```

---

<a id="ref-8.6-first-last"></a>
## `first()` / `last()`

```python
employees_df.groupBy("department_id").agg(
    F.first("name").alias("first_name")
)
```

**Pitfall:** Without an ordering concept, `first()`/`last()` are not a substitute for deterministic "earliest/latest" business logic. Use windows when order matters.

---

<a id="ref-8.7-collect_list-collect_set"></a>
## `collect_list()` / `collect_set()`

**Purpose:** Collect grouped values into arrays.

```python
orders_df.groupBy("customer_id").agg(
    F.collect_list("status").alias("statuses"),
    F.collect_set("status").alias("unique_statuses")
)
```

**Pitfall:** Large groups can create very large arrays in executor memory.

---

<a id="ref-8.8-conditional-aggregation"></a>
## Conditional aggregation

```python
orders_df.groupBy("customer_id").agg(
    F.sum(
        F.when(F.col("status") == "COMPLETE", F.col("amount"))
         .otherwise(0)
    ).alias("completed_amount")
)
```

---

<a id="ref-9-09-joins"></a>
# 09. Joins

<a id="ref-9.1-basic-join"></a>
## Basic `join()`

**Import:**

```python
from pyspark.sql import functions as F
```

**Syntax:**

```python
left.join(right, join_condition, how="inner")
```

**Example:**

```python
employees_df.join(
    departments_df,
    "department_id",
    "inner"
)
```

**SQL:**

```sql
SELECT *
FROM employees e
JOIN departments d
  ON e.department_id = d.department_id;
```

---

<a id="ref-9.2-join-types"></a>
## Join Types

<a id="ref-9.3-inner"></a>
### Inner

Only matching rows.

```python
left.join(right, "id", "inner")
```

<a id="ref-9.4-left"></a>
### Left

All left rows plus matching right rows.

```python
left.join(right, "id", "left")
```

<a id="ref-9.5-right"></a>
### Right

All right rows plus matching left rows.

```python
left.join(right, "id", "right")
```

<a id="ref-9.6-full-full-outer"></a>
### Full / Full Outer

All rows from both sides.

```python
left.join(right, "id", "full")
```

<a id="ref-9.7-left-semi"></a>
### Left Semi

Return left rows that have a match on the right; no right columns.

```python
orders_df.join(customers_df, "customer_id", "left_semi")
```

**Use:** Existence filtering.

<a id="ref-9.8-left-anti"></a>
### Left Anti

Return left rows with no match on the right.

```python
orders_df.join(customers_df, "customer_id", "left_anti")
```

**Use:** Referential-integrity / orphan checks.

<a id="ref-9.9-cross"></a>
### Cross

Cartesian product.

```python
df1.crossJoin(df2)
```

**Pitfall:** Can explode row counts dramatically.

---

<a id="ref-9.10-multiple-column-join"></a>
## Multiple-column join

```python
orders_df.join(
    customers_df,
    [
        orders_df.customer_id == customers_df.customer_id,
        orders_df.city == customers_df.city
    ],
    "left"
)
```

---

<a id="ref-9.11-join-expression-aliases"></a>
## Join expression + aliases

```python
e = employees_df.alias("e")
d = departments_df.alias("d")

result = e.join(
    d,
    F.col("e.department_id") == F.col("d.department_id"),
    "left"
).select(
    "e.name",
    "d.department"
)
```

**Remember:** Aliases are especially useful when both DataFrames contain same-named columns.

---

<a id="ref-9.12-duplicate-join-columns"></a>
## Duplicate join columns

If using:

```python
df1.join(df2, "customer_id")
```

Spark can keep one shared join key.

For explicit conditions, select/rename the desired columns afterward:

```python
a = orders_df.alias("a")
b = customers_df.alias("b")

result = a.join(
    b,
    F.col("a.customer_id") == F.col("b.customer_id")
).select(
    "a.*",
    F.col("b.customer_name")
)
```

---

<a id="ref-9.13-broadcast-join"></a>
## Broadcast join

**Import:**

```python
from pyspark.sql.functions import broadcast
```

**Purpose:** Replicates a small DataFrame to executors to avoid shuffling the large side.

**Example:**

```python
orders_df.join(
    broadcast(customers_df),
    "customer_id",
    "left"
)
```

**Problem → Solution:**

> Large fact table + genuinely small dimension → broadcast the dimension.

**Pitfall:** Broadcasting a table that is too large can cause executor memory pressure.

**Interview:** ⭐⭐⭐⭐⭐  
**Production:** ⭐⭐⭐⭐⭐

---

<a id="ref-10-10-window-functions"></a>
# 10. Window Functions

<a id="ref-10.1-window"></a>
## `Window`

**Purpose:** Defines how analytical calculations are partitioned and ordered without collapsing rows.

**Import:**

```python
from pyspark.sql.window import Window
```

**Example:**

```python
w = Window.partitionBy("customer_id").orderBy("order_date")
```

---

<a id="ref-10.2-partitionby"></a>
## `partitionBy()`

```python
w = Window.partitionBy("customer_id")
```

Defines independent groups for the window calculation.

---

<a id="ref-10.3-orderby"></a>
## `orderBy()`

```python
w = Window.partitionBy("customer_id").orderBy(
    F.col("order_date").desc()
)
```

Defines row order within each window partition.

---

<a id="ref-10.4-rowsbetween"></a>
## `rowsBetween()`

**Purpose:** Defines a row-based frame.

**Example:**

```python
w = (
    Window.partitionBy("customer_id")
    .orderBy("order_date")
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)
)
```

Useful for running aggregates.

---

<a id="ref-10.5-rangebetween"></a>
## `rangeBetween()`

**Purpose:** Defines a value-based range frame relative to the ordered expression.

```python
w = (
    Window.orderBy("order_date")
    .rangeBetween(Window.unboundedPreceding, Window.currentRow)
)
```

**Remember:** `rowsBetween()` counts physical rows; `rangeBetween()` works from ordering values and can include peers with the same ordering value.

---

<a id="ref-10.6-row_number"></a>
## `row_number()`

**Purpose:** Assigns unique sequential numbers within each window partition.

**Import:**

```python
from pyspark.sql.functions import row_number
```

**Example: Latest record per customer**

```python
w = Window.partitionBy("customer_id").orderBy(
    F.col("transaction_ts").desc()
)

latest = (
    transactions_df
    .withColumn("rn", row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)
```

**Interview:** ⭐⭐⭐⭐⭐  
**Production:** ⭐⭐⭐⭐⭐

---

<a id="ref-10.7-rank"></a>
## `rank()`

Ties receive the same rank and create gaps.

```python
w = Window.orderBy(F.col("salary").desc())
employees_df.withColumn("rank", F.rank().over(w))
```

---

<a id="ref-10.8-dense_rank"></a>
## `dense_rank()`

Ties receive the same rank without gaps.

```python
employees_df.withColumn(
    "dense_rank",
    F.dense_rank().over(w)
)
```

---

<a id="ref-10.9-percent_rank"></a>
## `percent_rank()`

Relative rank from 0 to 1.

```python
employees_df.withColumn(
    "percent_rank",
    F.percent_rank().over(w)
)
```

---

<a id="ref-10.10-lag-lead"></a>
## `lag()` / `lead()`

**Purpose:** Access previous/next rows within a window.

```python
w = Window.partitionBy("customer_id").orderBy("order_date")

df = orders_df.withColumn(
    "previous_amount",
    F.lag("amount").over(w)
).withColumn(
    "next_amount",
    F.lead("amount").over(w)
)
```

**Common use:** Change detection, period-over-period comparison, event sequencing.

---

<a id="ref-10.11-window-first-last"></a>
## Window `first()` / `last()`

```python
w = Window.partitionBy("customer_id").orderBy("order_date")

df = orders_df.withColumn(
    "first_amount",
    F.first("amount").over(w)
)
```

For deterministic latest/earliest business logic, make ordering and frame explicit.

---

<a id="ref-10.12-running-aggregate"></a>
## Running aggregate

```python
w = (
    Window.partitionBy("customer_id")
    .orderBy("order_date")
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)
)

df = orders_df.withColumn(
    "running_amount",
    F.sum("amount").over(w)
)
```

---

<a id="ref-10.13-groupby-vs-window"></a>
## GroupBy vs Window

**GroupBy:**

```python
orders_df.groupBy("customer_id").agg(F.sum("amount"))
```

Returns one row per group.

**Window:**

```python
orders_df.withColumn(
    "customer_total",
    F.sum("amount").over(Window.partitionBy("customer_id"))
)
```

Keeps the original rows and adds the analytical result.

---

<a id="ref-11-11-string-functions"></a>
# 11. String Functions

<a id="ref-11.1-split"></a>
## `split()`

**Import:**

```python
from pyspark.sql.functions import split
```

**Example:**

```python
df.withColumn("parts", split("name", " "))
```

---

<a id="ref-11.2-concat_ws"></a>
## `concat_ws()`

**Purpose:** Concatenates strings with a separator.

```python
df.withColumn(
    "full_address",
    F.concat_ws(", ", "city", "state", "country")
)
```

---

<a id="ref-11.3-substring"></a>
## `substring()`

```python
df.select(
    F.substring("customer_id", 1, 4).alias("prefix")
)
```

---

<a id="ref-11.4-translate"></a>
## `translate()`

**Purpose:** Character-by-character replacement.

```python
df.withColumn(
    "clean_phone",
    F.translate("phone", "()- ", "")
)
```

---

<a id="ref-11.5-regexp_replace"></a>
## `regexp_replace()`

**Purpose:** Regex-based replacement; useful for cleansing identifiers/text.

**Import:**

```python
from pyspark.sql.functions import regexp_replace
```

**Example:**

```python
df.withColumn(
    "clean_name",
    regexp_replace("name", r"[^A-Za-z0-9 ]", "")
)
```

**Production:** ⭐⭐⭐⭐⭐

---

<a id="ref-11.6-overlay"></a>
## `overlay()`

**Purpose:** Replaces a substring at a specified position.

```python
df.select(
    F.overlay("phone", F.lit("XXXX"), 4, 4).alias("masked_phone")
)
```

---

<a id="ref-11.7-other-high-value-string-functions"></a>
## Other high-value string functions

```python
F.lower("name")
F.upper("name")
F.trim("name")
F.ltrim("name")
F.rtrim("name")
F.length("name")
F.instr("name", "a")
F.substring_index("email", "@", 1)
```

**Remember:** Prefer Spark built-ins over Python UDFs for text cleansing.

---

<a id="ref-12-12-date-timestamp-functions"></a>
# 12. Date & Timestamp Functions

<a id="ref-12.1-to_date"></a>
## `to_date()`

**Import:**

```python
from pyspark.sql.functions import to_date
```

**Example:**

```python
df.withColumn(
    "order_date",
    to_date("order_ts", "yyyy-MM-dd HH:mm:ss")
)
```

---

<a id="ref-12.2-to_timestamp"></a>
## `to_timestamp()`

```python
df.withColumn(
    "event_ts",
    F.to_timestamp("event_time", "yyyy-MM-dd HH:mm:ss")
)
```

---

<a id="ref-12.3-date_format"></a>
## `date_format()`

```python
df.withColumn(
    "month_key",
    F.date_format("order_date", "yyyy-MM")
)
```

**Remember:** `date_format()` returns a string.

---

<a id="ref-12.4-datediff"></a>
## `datediff()`

```python
df.withColumn(
    "days_open",
    F.datediff("end_date", "start_date")
)
```

---

<a id="ref-12.5-months_between"></a>
## `months_between()`

```python
df.withColumn(
    "months_active",
    F.months_between("end_date", "start_date")
)
```

---

<a id="ref-12.6-extractors"></a>
## Extractors

```python
F.year("order_date")
F.month("order_date")
F.dayofmonth("order_date")
F.dayofweek("order_date")
F.hour("event_ts")
F.minute("event_ts")
F.second("event_ts")
```

---

<a id="ref-12.7-date_add-date_sub"></a>
## `date_add()` / `date_sub()`

```python
F.date_add("order_date", 7)
F.date_sub("order_date", 7)
```

---

<a id="ref-12.8-current-date-time"></a>
## Current date/time

```python
F.current_date()
F.current_timestamp()
```

**Pitfall:** Be explicit about timezone assumptions in production pipelines.

---

<a id="ref-13-13-array-functions"></a>
# 13. Array Functions

<a id="ref-13.1-array"></a>
## `array()`

**Import:**

```python
from pyspark.sql.functions import array
```

**Example:**

```python
df.withColumn(
    "locations",
    array("city", "state")
)
```

---

<a id="ref-13.2-explode"></a>
## `explode()`

**Purpose:** Converts each array/map element into a separate row.

**Example:**

```python
df.select(
    "customer_id",
    F.explode("items").alias("item")
)
```

**Pitfall:** Exploding a large array can multiply row counts dramatically.

**Production:** ⭐⭐⭐⭐⭐

---

<a id="ref-13.3-array_contains"></a>
## `array_contains()`

```python
df.filter(
    F.array_contains("tags", "priority")
)
```

---

<a id="ref-13.4-collect_list-collect_set"></a>
## `collect_list()` / `collect_set()`

Covered under aggregations; they return arrays from grouped values.

---

<a id="ref-13.5-other-high-value-array-functions"></a>
## Other high-value array functions

```python
F.size("items")
F.array_distinct("items")
F.array_sort("items")
F.array_join("items", ",")
F.element_at("items", 1)
F.flatten("nested_items")
F.array_union("a", "b")
```

---

<a id="ref-14-14-map-functions"></a>
# 14. Map Functions

<a id="ref-14.1-maptype"></a>
## `MapType`

**Import:**

```python
from pyspark.sql.types import MapType, StringType, IntegerType
```

**Example:**

```python
schema = MapType(StringType(), IntegerType())
```

---

<a id="ref-14.2-create_map"></a>
## `create_map()`

```python
df.withColumn(
    "attributes",
    F.create_map(
        F.lit("priority"), F.lit(1),
        F.lit("standard"), F.lit(0)
    )
)
```

---

<a id="ref-14.3-map_keys-map_values"></a>
## `map_keys()` / `map_values()`

```python
df.select(
    F.map_keys("attributes"),
    F.map_values("attributes")
)
```

---

<a id="ref-14.4-other-useful-map-operations"></a>
## Other useful map operations

```python
F.element_at("attributes", F.lit("priority"))
F.map_from_arrays("keys", "values")
F.map_concat("map1", "map2")
```

---

<a id="ref-15-15-struct-nested-data"></a>
# 15. Struct & Nested Data

<a id="ref-15.1-struct"></a>
## `struct()`

**Purpose:** Creates a nested struct column.

**Import:**

```python
from pyspark.sql.functions import struct
```

**Example:**

```python
df.withColumn(
    "employee",
    F.struct("employee_id", "name", "salary")
)
```

---

<a id="ref-15.2-nested-field-access"></a>
## Nested field access

```python
df.select("employee.name")
```

or:

```python
df.select(F.col("employee.name"))
```

---

<a id="ref-15.3-structtype-structfield"></a>
## `StructType` / `StructField`

```python
schema = StructType([
    StructField(
        "employee",
        StructType([
            StructField("id", IntegerType()),
            StructField("name", StringType())
        ])
    )
])
```

**Production use:** Useful for nested JSON/event data.

---

<a id="ref-16-16-json-functions"></a>
# 16. JSON Functions

<a id="ref-16.1-from_json"></a>
## `from_json()`

**Purpose:** Parses a JSON string into a typed struct/map/array.

**Import:**

```python
from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
```

**Example:**

```python
schema = StructType([
    StructField("id", IntegerType()),
    StructField("city", StringType())
])

parsed = df.withColumn(
    "payload",
    from_json("json_string", schema)
).select("payload.id", "payload.city")
```

**Production:** ⭐⭐⭐⭐⭐

---

<a id="ref-16.2-to_json"></a>
## `to_json()`

**Purpose:** Serializes a struct/map/array into JSON text.

```python
df.select(
    F.to_json(F.struct("customer_id", "amount")).alias("payload")
)
```

---

<a id="ref-16.3-schema_of_json"></a>
## `schema_of_json()`

**Purpose:** Infers a Spark schema from a JSON literal.

```python
schema = F.schema_of_json(
    F.lit('{"id":1,"city":"Hyderabad"}')
)
```

**Remember:** Useful for controlled/deterministic sample JSON; production ingestion should generally use a managed/explicit schema.

---

<a id="ref-16.4-json_tuple"></a>
## `json_tuple()`

**Purpose:** Extracts fields from JSON strings.

```python
df.select(
    F.json_tuple("json_string", "id", "city")
    .alias("id", "city")
)
```

**Guidance:** For typed nested processing, prefer `from_json()`.

---

<a id="ref-16.5-get_json_object"></a>
## `get_json_object()`

**Purpose:** Extracts a JSON path as a string.

```python
df.select(
    F.get_json_object("json_string", "$.customer.city")
)
```

**Guidance:** Good for targeted extraction; use `from_json()` when the structure is reused.

---

<a id="ref-17-17-null-handling"></a>
# 17. Null Handling

<a id="ref-17.1-isnull-isnotnull"></a>
## `isNull()` / `isNotNull()`

```python
df.filter(F.col("amount").isNull())
df.filter(F.col("amount").isNotNull())
```

---

<a id="ref-17.2-fillna"></a>
## `fillna()`

```python
df.fillna({
    "amount": 0,
    "status": "UNKNOWN"
})
```

---

<a id="ref-17.3-coalesce-function"></a>
## `coalesce()` function

```python
df.withColumn(
    "amount",
    F.coalesce("amount", F.lit(0))
)
```

---

<a id="ref-17.4-when-otherwise"></a>
## `when()` / `otherwise()`

```python
df.withColumn(
    "status",
    F.when(F.col("status").isNull(), "UNKNOWN")
     .otherwise(F.col("status"))
)
```

---

<a id="ref-17.5-null-safe-comparison"></a>
## Null-safe comparison

```python
df.filter(F.expr("a <=> b"))
```

---

<a id="ref-18-18-udfs"></a>
# 18. UDFs

<a id="ref-18.1-python-udf"></a>
## Python UDF

**Purpose:** Applies custom Python logic where Spark built-ins cannot express the required transformation.

**Import:**

```python
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
```

**Example:**

```python
@udf(returnType=StringType())
def normalize_code(x):
    return x.strip().upper() if x else None

df.withColumn("normalized_code", normalize_code("code"))
```

**Remember:** Python UDFs can introduce serialization/Python execution overhead and limit Spark's ability to optimize the operation.

---

<a id="ref-18.2-built-in-function-vs-udf"></a>
## Built-in function vs UDF

**Prefer:**

```python
F.upper(F.trim("code"))
```

over a Python UDF when equivalent functionality exists.

**Rule:**

> Built-in Spark function > SQL expression > Pandas/vectorized UDF > regular Python UDF, when functionality and semantics are comparable.

---

<a id="ref-18.3-pandas-udf"></a>
## Pandas UDF

**Purpose:** Vectorized Python UDF using Pandas/Arrow for supported workloads.

**Import:**

```python
from pyspark.sql.functions import pandas_udf
```

**Example:**

```python
@pandas_udf("double")
def add_tax(amount):
    return amount * 1.18
```

**Remember:** Vectorization can reduce Python-call overhead, but it is still usually less optimizer-friendly than native Spark expressions.

---

<a id="ref-18.4-applyinpandas"></a>
## `applyInPandas()`

**Purpose:** Applies grouped Pandas logic to each group.

```python
result = (
    orders_df
    .groupBy("customer_id")
    .applyInPandas(my_function, schema)
)
```

**Use only when the grouped computation genuinely requires Python/Pandas logic.**

---

<a id="ref-19-19-data-sources"></a>
# 19. Data Sources

<a id="ref-19.1-general-read-pattern"></a>
## General read pattern

```python
df = (
    spark.read
    .format("parquet")
    .option("key", "value")
    .load("/path")
)
```

<a id="ref-19.2-general-write-pattern"></a>
## General write pattern

```python
(
    df.write
    .format("parquet")
    .mode("overwrite")
    .save("/output")
)
```

Common modes:

```text
append
overwrite
error / errorifexists
ignore
```

---

<a id="ref-19.3-csv"></a>
## CSV

**Read:**

```python
df = (
    spark.read
    .option("header", True)
    .option("inferSchema", False)
    .schema(schema)
    .csv("/data/orders.csv")
)
```

**Write:**

```python
df.write.mode("overwrite").option("header", True).csv("/out/orders")
```

**Production:** Prefer explicit schemas over `inferSchema` for controlled pipelines.

---

<a id="ref-19.4-parquet"></a>
## Parquet

**Read:**

```python
df = spark.read.parquet("/data/orders")
```

**Write:**

```python
df.write.mode("overwrite").parquet("/out/orders")
```

**Remember:** Columnar, compressed, schema-aware, and generally preferred for analytical Spark workloads.

**Production:** ⭐⭐⭐⭐⭐

---

<a id="ref-19.5-json"></a>
## JSON

**Read:**

```python
df = spark.read.json("/data/events.json")
```

**Write:**

```python
df.write.mode("overwrite").json("/out/events")
```

---

<a id="ref-19.6-hive"></a>
## Hive

**Read table:**

```python
df = spark.table("analytics.orders")
```

**SQL:**

```python
spark.sql("SELECT * FROM analytics.orders")
```

**Remember:** Hive support/catalog behavior depends on Spark deployment configuration.

---

<a id="ref-19.7-jdbc"></a>
## JDBC

**Purpose:** Read/write relational databases such as SQL Server and MySQL.

**Read:**

```python
df = (
    spark.read
    .format("jdbc")
    .option("url", jdbc_url)
    .option("dbtable", "dbo.orders")
    .option("user", username)
    .option("password", password)
    .load()
)
```

<a id="ref-19.8-query"></a>
### Query

```python
.option(
    "dbtable",
    "(SELECT * FROM orders WHERE status = 'COMPLETE') AS q"
)
```

<a id="ref-19.9-parallel-jdbc-read"></a>
### Parallel JDBC read

```python
df = (
    spark.read
    .format("jdbc")
    .option("url", jdbc_url)
    .option("dbtable", "dbo.orders")
    .option("partitionColumn", "order_id")
    .option("lowerBound", 1)
    .option("upperBound", 10000000)
    .option("numPartitions", 16)
    .option("user", username)
    .option("password", password)
    .load()
)
```

**Important:** `partitionColumn`, `lowerBound`, and `upperBound` define parallel read ranges; they do not filter rows by themselves.

**Pitfall:** Too many JDBC partitions can overload the source database.

---

<a id="ref-19.10-sql-server"></a>
## SQL Server

Use JDBC with a SQL Server driver/JDBC URL.

```python
jdbc_url = "jdbc:sqlserver://host:1433;databaseName=analytics"
```

Then use the standard JDBC read/write pattern.

---

<a id="ref-19.11-mysql"></a>
## MySQL

```python
jdbc_url = "jdbc:mysql://host:3306/analytics"
```

Use the standard JDBC read/write pattern.

---

<a id="ref-19.12-partitioned-write-partitionby"></a>
## Partitioned write: `partitionBy()`

**Purpose:** Writes files into directory partitions based on column values.

**Example:**

```python
(
    orders_df.write
    .mode("overwrite")
    .partitionBy("order_date")
    .parquet("/warehouse/orders")
)
```

Produces a layout conceptually like:

```text
orders/
  order_date=2026-08-01/
  order_date=2026-08-02/
```

**Remember:** This is a storage layout operation, not the same thing as `df.repartition()`.

---

<a id="ref-20-20-partitioning"></a>
# 20. Partitioning

<a id="ref-20.1-partition-concept"></a>
## Partition concept

A DataFrame is distributed across partitions. Normally one task processes one partition at a time.

**Too few partitions:** poor parallelism.

**Too many/small partitions:** scheduling overhead and small output files.

---

<a id="ref-20.2-repartition"></a>
## `repartition()`

**Purpose:** Changes the number/distribution of partitions, normally with a shuffle.

**Import:** DataFrame method; no function import required.

**Syntax:**

```python
df.repartition(numPartitions)
df.repartition("key")
df.repartition(numPartitions, "key")
```

**Example:**

```python
orders_df.repartition(20, "customer_id")
```

**Use:** Need to redistribute data, often before expensive keyed operations or for output balancing.

**Pitfall:** Shuffle can be expensive.

---

<a id="ref-20.3-coalesce"></a>
## `coalesce()`

**Purpose:** Reduces partitions, generally avoiding a full shuffle.

**Example:**

```python
orders_df.coalesce(4)
```

**Use:** Reducing partition count, especially before writing a small result.

**Pitfall:** Aggressive coalescing can reduce parallelism.

---

<a id="ref-20.4-partitionby"></a>
## `partitionBy()`

**Purpose:** Controls directory/file partitioning when writing data.

```python
df.write.partitionBy("year", "month").parquet("/warehouse/events")
```

**Key distinction:**

| API | Main purpose | Shuffle? |
|---|---|---|
| `repartition()` | Redistribute execution partitions | Usually yes |
| `coalesce()` | Reduce execution partitions | Usually avoids full shuffle |
| `partitionBy()` | Organize output files/directories | May involve redistribution during write |

---

<a id="ref-20.5-hash-partitioning"></a>
## Hash partitioning

Conceptually:

```text
partition = hash(key) % number_of_partitions
```

Spark can use hash partitioning for joins, aggregations, and repartitioning.

---

<a id="ref-20.6-partition-pruning"></a>
## Partition pruning

**Purpose:** Avoids reading irrelevant storage partitions.

Example layout:

```text
orders/year=2025/month=12/
orders/year=2026/month=01/
```

Query:

```python
spark.read.parquet("/warehouse/orders") \
    .filter("year = 2026 AND month = 1")
```

**Benefit:** Reads only relevant partitions when the source/query plan can apply the partition filter.

**Production:** ⭐⭐⭐⭐⭐

---

<a id="ref-21-21-performance-optimization"></a>
# 21. Performance Optimization

<a id="ref-21.1-optimization-decision-order"></a>
## Optimization decision order

1. Read only required columns.
2. Filter early when useful.
3. Use appropriate storage format.
4. Ensure partition pruning.
5. Inspect joins and shuffle.
6. Handle skew.
7. Tune partition counts.
8. Cache only reused expensive data.
9. Avoid unnecessary Python UDFs.
10. Validate with `explain()` and Spark UI.

---

<a id="ref-21.2-explain"></a>
## `explain()`

**Purpose:** Shows the logical/physical execution plan.

**Import:** DataFrame method.

**Example:**

```python
orders_df.filter(F.col("amount") > 100).explain("formatted")
```

Useful modes include:

```python
df.explain()
df.explain("formatted")
df.explain("extended")
```

**Look for:** scans, filters, exchanges/shuffles, join strategies, broadcast, partition/file pruning.

**Interview:** ⭐⭐⭐⭐⭐  
**Production:** ⭐⭐⭐⭐⭐

---

<a id="ref-21.3-predicate-pushdown"></a>
## Predicate pushdown

**Problem:** Spark reads more source data than necessary.

**Preferred solution:** Use a source/format that can push filters down.

```python
df = (
    spark.read.parquet("/data/orders")
    .filter(F.col("status") == "COMPLETE")
)
```

**Why:** Compatible data sources can apply the filter during/near the scan.

**Remember:** Predicate pushdown is source/format dependent; not every transformation can be pushed down.

---

<a id="ref-21.4-column-pruning"></a>
## Column pruning

**Problem:** Reading unnecessary columns.

**Preferred solution:**

```python
df = spark.read.parquet("/data/orders").select(
    "order_id", "customer_id", "amount"
)
```

**Why:** Columnar formats can often avoid reading unused columns.

---

<a id="ref-21.5-broadcast-join"></a>
## Broadcast join

Covered above.

**Problem:** Large shuffle join with a small lookup table.

**Preferred solution:**

```python
large.join(F.broadcast(small), "id")
```

---

<a id="ref-21.6-shuffle"></a>
## Shuffle

**Problem:** Data must move between executors.

Common causes:

```python
groupBy()
join()
distinct()
orderBy()
repartition()
```

**Why it matters:** Network transfer, serialization, disk spill, and coordination make shuffle expensive.

**Preferred approach:** Reduce unnecessary shuffles and make partitioning/join strategy deliberate.

---

<a id="ref-21.7-data-skew"></a>
## Data skew

**Problem:** A few keys contain disproportionately large amounts of data.

**Symptom:** Most tasks finish quickly while a few tasks run much longer.

**Why:** One or more partitions become much larger than others.

**Solutions:**

- Broadcast small dimension tables.
- Filter unnecessary records.
- Reconsider join strategy.
- Salt extreme keys when appropriate.
- Pre-aggregate where valid.
- Use adaptive query execution features where available.
- Investigate skew in Spark UI.

**Interview:** ⭐⭐⭐⭐⭐  
**Production:** ⭐⭐⭐⭐⭐

---

<a id="ref-21.8-adaptive-query-execution-aqe"></a>
## Adaptive Query Execution (AQE)

**Purpose:** Lets Spark adapt parts of the physical plan using runtime statistics.

Common benefits include:

- Coalescing post-shuffle partitions.
- Handling skewed joins.
- Dynamically changing join strategy in supported situations.

**Remember:** AQE is a modern Spark optimization; understand what it can adapt rather than assuming it fixes every performance issue.

---

<a id="ref-21.9-cache-cache"></a>
## Cache / `cache()`

**Purpose:** Keeps reused computed data in memory/storage according to the caching policy.

**Example:**

```python
cleaned = raw_df.filter(F.col("is_valid") == True).cache()

cleaned.count()
cleaned.groupBy("type").count().show()
```

**Use:** When the same expensive DataFrame is reused multiple times.

**Pitfall:** Caching data used once wastes resources.

---

<a id="ref-21.10-persist"></a>
## `persist()`

**Purpose:** Caches with an explicit storage level.

**Import:**

```python
from pyspark import StorageLevel
```

**Example:**

```python
df.persist(StorageLevel.MEMORY_AND_DISK)
```

**Remember:** Choose persistence deliberately when memory alone is not suitable.

---

<a id="ref-21.11-unpersist"></a>
## `unpersist()`

```python
df.unpersist()
```

Releases cached/persisted data when no longer needed.

---

<a id="ref-21.12-file-sizes-small-file-problem"></a>
## File sizes / small-file problem

**Problem:** Thousands/millions of tiny output files create metadata and scheduling overhead.

**Common causes:**

- Excessive partition counts.
- Over-partitioned writes.
- Frequent incremental writes.
- High-cardinality `partitionBy()`.

**Preferred solution:** Control output partition counts and target sensible file sizes for the storage/query engine.

Example:

```python
df.coalesce(20).write.mode("append").parquet("/out")
```

**Caution:** Do not blindly use `coalesce(1)` for production-scale data.

---

<a id="ref-21.13-driver-memory"></a>
## Driver memory

**Problem:** Too much data is brought to the driver.

**Risky:**

```python
df.collect()
df.toPandas()
```

**Preferred:**

```python
df.limit(100).show()
```

or aggregate/filter before collecting.

---

<a id="ref-21.14-built-ins-vs-udf"></a>
## Built-ins vs UDF

**Problem:** Python UDF introduces Python execution overhead and can restrict optimizer visibility.

**Preferred:** Use native Spark SQL functions.

---

<a id="ref-21.15-join-optimization"></a>
## Join optimization

Check:

```python
df.explain("formatted")
```

Ask:

- Is a broadcast join appropriate?
- Is the join key skewed?
- Are unnecessary columns being carried?
- Is the join filtering enough rows?
- Is the source partitioned appropriately?

---

<a id="ref-21.16-partition-count"></a>
## Partition count

**Problem:** Poor task parallelism or too much task overhead.

**Preferred approach:** Tune based on data volume, cluster resources, shuffle size, and observed Spark UI behavior—not a universal fixed number.

---

<a id="ref-22-22-spark-ui-debugging"></a>
# 22. Spark UI & Debugging

<a id="ref-22.1-jobs-tab"></a>
## Jobs tab

Look for:

- Number of jobs.
- Duration.
- Failed jobs.
- Which action triggered the job.

<a id="ref-22.2-stages-tab"></a>
## Stages tab

Look for:

- Shuffle read/write.
- Stage duration.
- Task distribution.
- Failed/retried tasks.

<a id="ref-22.3-tasks"></a>
## Tasks

Look for:

- One task much slower than peers → possible skew.
- Large input/shuffle → data movement issue.
- High spill → memory/partition pressure.

<a id="ref-22.4-sql-tab"></a>
## SQL tab

Use it to inspect:

- SQL/DataFrame execution.
- Physical plan.
- Operator timings.
- Join strategy.
- Exchange/shuffle operators.

<a id="ref-22.5-executors-tab"></a>
## Executors tab

Look for:

- Executor failures.
- Memory usage.
- GC time.
- Active/failed tasks.

<a id="ref-22.6-storage-tab"></a>
## Storage tab

Useful when diagnosing cached/persisted DataFrames.

<a id="ref-22.7-slow-job-checklist"></a>
### Slow-job checklist

```text
1. Identify slow stage.
2. Inspect shuffle read/write.
3. Compare task durations.
4. Look for skewed tasks.
5. Inspect physical plan.
6. Check join strategy.
7. Check partition count.
8. Check executor memory/GC.
9. Validate input file sizes.
10. Apply one targeted optimization and measure again.
```

---

<a id="ref-23-23-data-engineering-patterns"></a>
# 23. Data Engineering Patterns

<a id="ref-23.1-duplicate-detection"></a>
## Duplicate detection

```python
duplicates = (
    orders_df
    .groupBy("order_id")
    .count()
    .filter(F.col("count") > 1)
)
```

---

<a id="ref-23.2-null-validation"></a>
## Null validation

```python
null_counts = orders_df.select([
    F.sum(F.col(c).isNull().cast("int")).alias(c)
    for c in orders_df.columns
])
```

---

<a id="ref-23.3-referential-integrity"></a>
## Referential integrity

**Problem:** Fact rows reference missing dimensions.

```python
orphans = orders_df.join(
    customers_df,
    "customer_id",
    "left_anti"
)
```

---

<a id="ref-23.4-record-count-validation"></a>
## Record-count validation

```python
source_count = source_df.count()
target_count = target_df.count()

assert source_count == target_count
```

**Production note:** Count equality alone does not prove data correctness.

---

<a id="ref-23.5-invalid-date-detection"></a>
## Invalid-date detection

```python
validated = df.withColumn(
    "parsed_date",
    F.to_date("raw_date", "yyyy-MM-dd")
)

invalid = validated.filter(
    F.col("raw_date").isNotNull() &
    F.col("parsed_date").isNull()
)
```

---

<a id="ref-23.6-schema-validation"></a>
## Schema validation

```python
expected = StructType([
    StructField("id", LongType(), False),
    StructField("amount", DoubleType(), True)
])

assert df.schema == expected
```

For production systems, prefer explicit validation rules that tolerate intentional schema evolution where required.

---

<a id="ref-23.7-incremental-filtering"></a>
## Incremental filtering

```python
incremental = source_df.filter(
    F.col("updated_at") > F.lit(last_watermark)
)
```

**Remember:** Production incremental loads need a reliable watermark/key strategy and idempotency.

---

<a id="ref-23.8-latest-record-per-key"></a>
## Latest record per key

```python
w = Window.partitionBy("customer_id").orderBy(
    F.col("updated_at").desc(),
    F.col("ingested_at").desc()
)

latest = (
    df.withColumn("rn", F.row_number().over(w))
      .filter("rn = 1")
      .drop("rn")
)
```

---

<a id="ref-23.9-scd-type-1-conceptual-pattern"></a>
## SCD Type 1 — conceptual pattern

Overwrite the current dimension attribute when the source changes.

Typical flow:

```text
Source → match business key → update changed attributes / insert new keys
```

Use merge/upsert semantics in the target system where supported.

---

<a id="ref-23.10-scd-type-2-conceptual-pattern"></a>
## SCD Type 2 — conceptual pattern

Preserve historical versions.

Typical columns:

```text
business_key
attribute
effective_from
effective_to
is_current
```

PySpark commonly prepares the change dataset; the target storage layer performs the merge/history operation.

---

<a id="ref-23.11-idempotent-transformation"></a>
## Idempotent transformation

**Goal:** Running the same pipeline twice should not create duplicate business results.

Typical strategies:

- Deterministic keys.
- Merge/upsert.
- Replace a partition.
- Deduplicate before write.
- Watermark + unique event key.
- Write to staging then atomically publish.

**Interview:** ⭐⭐⭐⭐⭐  
**Production:** ⭐⭐⭐⭐⭐

---

<a id="ref-24-24-recommended-missing-topics"></a>
# 24. Recommended Missing Topics

These are **additional topics**, not part of the original requested list. They are high-value for a modern Data Engineer.

<a id="ref-24.1-24-1-spark-sql"></a>
## 24.1 Spark SQL

```python
df.createOrReplaceTempView("orders")
spark.sql("""
    SELECT customer_id, SUM(amount) AS total
    FROM orders
    GROUP BY customer_id
""")
```

**Why learn:** DataFrames and SQL use the same underlying Spark SQL engine.

---

<a id="ref-24.2-24-2-catalyst-optimizer"></a>
## 24.2 Catalyst Optimizer

**Purpose:** Spark SQL's optimizer transforms logical plans into more efficient physical execution plans.

**Remember:** This is why DataFrame/Spark SQL expressions are generally preferable to opaque Python row-by-row logic.

---

<a id="ref-24.3-24-3-tungsten-whole-stage-code-generation"></a>
## 24.3 Tungsten / whole-stage code generation

High-level concept: Spark can generate efficient JVM execution code for many native operations.

**Interview:** ⭐⭐⭐⭐

---

<a id="ref-24.4-24-4-adaptive-query-execution"></a>
## 24.4 Adaptive Query Execution

Covered in performance. Know:

- skew handling
- post-shuffle partition coalescing
- runtime join strategy adaptation

---

<a id="ref-24.5-24-5-spark-sql-shuffle-partitions"></a>
## 24.5 `spark.sql.shuffle.partitions`

**Purpose:** Default number of shuffle partitions for many Spark SQL operations.

```python
spark.conf.get("spark.sql.shuffle.partitions")
```

Set deliberately based on workload:

```python
spark.conf.set("spark.sql.shuffle.partitions", 200)
```

**Remember:** Do not use a universal number; validate with Spark UI/AQE.

---

<a id="ref-24.6-24-6-broadcast-threshold"></a>
## 24.6 Broadcast threshold

Spark can automatically broadcast sufficiently small relations based on configuration.

```python
spark.conf.get("spark.sql.autoBroadcastJoinThreshold")
```

**Remember:** Manual `broadcast()` is useful when you know a relation is safely small and want to express the intent explicitly.

---

<a id="ref-24.7-24-7-checkpointing"></a>
## 24.7 Checkpointing

**Purpose:** Truncates long lineage and can improve fault recovery for very long dependency chains.

**Import:**

```python
from pyspark import SparkContext
```

DataFrame checkpointing is available through the Spark context/checkpoint configuration in supported Spark versions.

**Use:** Long iterative/lineage-heavy jobs; not a substitute for ordinary caching.

---

<a id="ref-24.8-24-8-broadcast-variable-vs-broadcast-join"></a>
## 24.8 Broadcast variable vs broadcast join

<a id="ref-24.9-broadcast-variable"></a>
### Broadcast variable

Sends a read-only Python object to executors.

```python
mapping = spark.sparkContext.broadcast({
    "A": "Active",
    "I": "Inactive"
})
```

<a id="ref-24.10-broadcast-join"></a>
### Broadcast join

Broadcasts a DataFrame relation for a join.

```python
large_df.join(F.broadcast(small_df), "id")
```

**Remember:** They solve different problems.

---

<a id="ref-24.11-24-9-accumulator"></a>
## 24.9 Accumulator

**Purpose:** Executor-side tasks can add to an accumulator value that is observable by the driver.

**Example:**

```python
bad_records = spark.sparkContext.longAccumulator("bad_records")
```

Then inside executor-side logic:

```python
bad_records.add(1)
```

**Use:** Monitoring/debug counters, not business-critical output.

**Pitfall:** Do not use accumulators as a source of truth for transformations; task retries can make side-effect reasoning tricky.

---

<a id="ref-24.12-24-10-structured-streaming"></a>
## 24.10 Structured Streaming

High-value modern topic.

Core concepts:

```text
readStream → transformations → writeStream
```

Example:

```python
events = (
    spark.readStream
    .format("json")
    .schema(schema)
    .load("/events")
)

query = (
    events.writeStream
    .format("parquet")
    .option("checkpointLocation", "/checkpoints/events")
    .start("/output/events")
)
```

Learn next:

- checkpoints
- output modes
- triggers
- watermarks
- late data
- stateful aggregations

---

<a id="ref-24.13-24-11-watermarking"></a>
## 24.11 Watermarking

**Purpose:** Bounds how long Spark keeps state for late-arriving event data in supported streaming operations.

```python
events.withWatermark("event_time", "10 minutes")
```

---

<a id="ref-24.14-24-12-schema-evolution"></a>
## 24.12 Schema evolution

Know the difference between:

- schema inference
- explicit schema
- additive changes
- incompatible changes
- controlled migration

**Production:** ⭐⭐⭐⭐⭐

---

<a id="ref-24.15-24-13-data-contracts"></a>
## 24.13 Data contracts

Define expected:

- columns
- data types
- nullability
- business constraints
- ownership
- freshness expectations

Useful for reliable upstream/downstream integration.

---

<a id="ref-24.16-24-14-observability"></a>
## 24.14 Observability

Track:

- input/output row counts
- rejected records
- null rates
- freshness
- duration
- partition counts
- data-quality failures
- lineage

This is especially important for production pipelines.

---

<a id="ref-24.17-24-15-testing"></a>
## 24.15 Testing

Recommended layers:

```text
Unit tests → transformation logic
Data-quality tests → constraints
Integration tests → source/target behavior
Regression tests → business logic
```

For PySpark, keep transformation logic modular and testable rather than embedding everything in one job script.

---

<a id="ref-24.18-24-16-secrets-credentials"></a>
## 24.16 Secrets / credentials

Do not hard-code:

```python
.option("password", "myPassword")
```

Use the deployment platform's secret manager/configuration mechanism.

---

<a id="ref-24.19-24-17-data-formats"></a>
## 24.17 Data formats

Know when to prefer:

- Parquet for analytical batch data.
- JSON for semi-structured interchange/events.
- CSV mainly for simple external/file exchange.
- JDBC for relational system integration.

---

<a id="ref-24.20-24-18-delta-iceberg-hudi-style-table-formats"></a>
## 24.18 Delta/Iceberg/Hudi-style table formats

For modern lakehouse environments, understand at least the concepts:

- ACID transactions
- schema evolution
- time travel
- merge/upsert
- partitioning
- compaction
- table optimization

The exact API depends on the table format/platform.

---

<a id="ref-25-requested-topics-validation"></a>
# Requested Topics Validation

The explicit requested topics are covered across the reference:

- Fundamentals, modules, cluster managers, Spark UI, SparkContext, RDD, `parallelize()`, `repartition()` vs `coalesce()`, broadcast variables, accumulators.
- DataFrame creation, empty DataFrame, RDD conversion, Pandas conversion, `show()`, schemas, columns, selection, collection, column operations, filtering, joins, unions, UDFs, `transform()`, sampling, null handling, pivot, `partitionBy()`, `MapType`.
- Aggregate, window, date/timestamp, and JSON functions.
- CSV, Parquet, JSON, Hive, JDBC, SQL Server, MySQL.
- `when()`, `expr()`, `lit()`, `split()`, `concat_ws()`, `substring()`, `translate()`, `regexp_replace()`, `overlay()`, date functions, array/map/struct functions, ranking/window functions, `typedLit()`, JSON functions.
- Execution, shuffle, partitioning, broadcast joins, skew, cache/persist, pushdown, pruning, execution plans, file sizing, driver risks, UDF guidance, Spark UI, data-quality patterns, and quick reference.

---

<a id="ref-26-25-pyspark-quick-reference-cheat-sheet"></a>
# 25. PySpark Quick Reference Cheat Sheet

<a id="ref-26.1-sparksession"></a>
## SparkSession

```text
SparkSession.builder.getOrCreate() → create/get Spark session
spark.sql(sql) → execute Spark SQL
spark.table("db.table") → load catalog table
```

<a id="ref-26.2-dataframe-creation"></a>
## DataFrame Creation

```text
spark.createDataFrame(data, schema) → create DataFrame
rdd.toDF(cols) → RDD to DataFrame
```

<a id="ref-26.3-inspection"></a>
## Inspection

```text
df.show() → display rows
df.printSchema() → display schema
df.schema → StructType schema
df.columns → list column names
df.explain("formatted") → execution plan
```

<a id="ref-26.4-selection"></a>
## Selection

```text
df.select(...) → select/project columns
df.selectExpr(...) → select using SQL expressions
F.col("x") → reference column
F.lit(x) → literal value
```

<a id="ref-26.5-filtering"></a>
## Filtering

```text
df.filter(condition) → filter rows
df.where(condition) → filter rows
col("x").isNull() → null check
col("x").isNotNull() → non-null check
expr("a <=> b") → null-safe equality
```

<a id="ref-26.6-column-operations"></a>
## Column Operations

```text
df.withColumn("x", expr) → add/replace column
df.withColumnRenamed("a", "b") → rename column
df.drop("x") → drop column
F.when(...).otherwise(...) → conditional expression
F.expr("...") → SQL expression
F.coalesce(a,b) → first non-null expression
F.typedLit(value) → typed literal
```

<a id="ref-26.7-aggregations"></a>
## Aggregations

```text
df.groupBy(...) → group rows
df.agg(...) → aggregate
F.count("*") → row count
F.countDistinct("x") → distinct count
F.sum("x") → sum
F.avg("x") → average
F.min("x") → minimum
F.max("x") → maximum
F.first("x") → first aggregate value
F.last("x") → last aggregate value
F.collect_list("x") → array including duplicates
F.collect_set("x") → unique array
```

<a id="ref-26.8-joins"></a>
## Joins

```text
df.join(other, "id", "inner") → inner join
df.join(other, "id", "left") → left join
df.join(other, "id", "right") → right join
df.join(other, "id", "full") → full outer join
df.join(other, "id", "left_semi") → matching left rows only
df.join(other, "id", "left_anti") → non-matching left rows
df.crossJoin(other) → Cartesian join
F.broadcast(df) → mark small DataFrame for broadcast join
```

<a id="ref-26.9-windows"></a>
## Windows

```text
Window.partitionBy(...) → window groups
Window.orderBy(...) → window ordering
Window.rowsBetween(...) → row-based frame
Window.rangeBetween(...) → value-range frame
F.row_number().over(w) → sequential row number
F.rank().over(w) → rank with gaps
F.dense_rank().over(w) → rank without gaps
F.percent_rank().over(w) → relative rank
F.lag("x").over(w) → previous row
F.lead("x").over(w) → next row
F.first("x").over(w) → first window value
F.last("x").over(w) → last window value
F.sum("x").over(w) → running/window aggregate
```

<a id="ref-26.10-strings"></a>
## Strings

```text
F.split("x", delimiter) → array from string
F.concat_ws(",", ...) → join strings
F.substring("x", start, len) → substring
F.translate("x", from, to) → character replacement
F.regexp_replace("x", pattern, replacement) → regex replacement
F.overlay(...) → replace substring by position
F.lower("x") → lowercase
F.upper("x") → uppercase
F.trim("x") → trim whitespace
F.length("x") → string length
```

<a id="ref-26.11-dates"></a>
## Dates

```text
F.to_date("x", format) → parse date
F.to_timestamp("x", format) → parse timestamp
F.date_format("x", format) → format date/timestamp as string
F.datediff(end, start) → day difference
F.months_between(end, start) → month difference
F.year("x") → year
F.month("x") → month
F.dayofmonth("x") → day
F.hour("x") → hour
F.date_add("x", n) → add days
F.date_sub("x", n) → subtract days
F.current_date() → current date
F.current_timestamp() → current timestamp
```

<a id="ref-26.12-arrays"></a>
## Arrays

```text
F.array(...) → create array
F.explode("x") → array/map element to rows
F.array_contains("x", value) → membership test
F.size("x") → array size
F.array_distinct("x") → remove duplicates
F.array_sort("x") → sort array
F.array_join("x", ",") → array to string
F.element_at("x", i) → array/map element
F.flatten("x") → flatten nested arrays
```

<a id="ref-26.13-maps"></a>
## Maps

```text
F.create_map(...) → create map
F.map_keys("x") → map keys
F.map_values("x") → map values
F.element_at("x", key) → map value
F.map_from_arrays(keys, values) → map from arrays
F.map_concat(a,b) → combine maps
```

<a id="ref-26.14-struct-nested"></a>
## Struct / Nested

```text
F.struct(...) → create struct
df.select("nested.field") → access nested field
StructType(...) → define struct schema
StructField(...) → define field
MapType(keyType, valueType) → map schema
ArrayType(elementType) → array schema
```

<a id="ref-26.15-json"></a>
## JSON

```text
F.from_json("json", schema) → JSON string to typed structure
F.to_json(expr) → structure to JSON string
F.json_tuple("json", ...) → extract JSON fields
F.get_json_object("json", "$.path") → JSON-path extraction
F.schema_of_json(F.lit(json)) → infer schema from sample JSON
```

<a id="ref-26.16-null-handling"></a>
## Null Handling

```text
col("x").isNull() → null rows
col("x").isNotNull() → non-null rows
df.fillna(...) → replace nulls
F.coalesce(a,b) → first non-null expression
F.when(...).otherwise(...) → conditional null handling
expr("a <=> b") → null-safe comparison
```

<a id="ref-26.17-partitioning"></a>
## Partitioning

```text
df.repartition(n) → redistribute into n partitions
df.repartition(n, "key") → hash repartition by key
df.coalesce(n) → reduce partitions with minimal redistribution
df.write.partitionBy("date") → directory/file partitioning
```

<a id="ref-26.18-performance"></a>
## Performance

```text
df.explain("formatted") → inspect physical plan
df.cache() → cache reused DataFrame
df.persist(level) → persist with storage level
df.unpersist() → release persistence
F.broadcast(df) → broadcast small join side
```

<a id="ref-26.19-read"></a>
## Read

```text
spark.read.csv(path) → CSV
spark.read.parquet(path) → Parquet
spark.read.json(path) → JSON
spark.read.format("jdbc").options(...).load() → JDBC
spark.table("db.table") → catalog/Hive table
```

<a id="ref-26.20-write"></a>
## Write

```text
df.write.mode("overwrite").parquet(path) → Parquet write
df.write.mode("append").json(path) → JSON write
df.write.partitionBy("date").parquet(path) → partitioned write
df.write.format("jdbc").options(...).mode("append").save() → JDBC write
```

<a id="ref-26.21-rdd"></a>
## RDD

```text
sc.parallelize(data) → create RDD
rdd.map(fn) → transform each element
rdd.flatMap(fn) → transform + flatten
rdd.filter(fn) → filter elements
rdd.foreach(fn) → executor-side side effect
rdd.collect() → bring all results to driver
rdd.repartition(n) → redistribute partitions
rdd.coalesce(n) → reduce partitions
rdd.toDF(...) → RDD to DataFrame
```

<a id="ref-26.22-high-value-interview-reminders"></a>
## High-Value Interview Reminders

```text
Transformation → lazy; builds execution plan
Action → triggers execution
Narrow → usually no shuffle
Wide → usually shuffle
repartition → redistribution/shuffle
coalesce → mainly reduce partitions
partitionBy → output directory layout
groupBy → reduces rows to groups
window → keeps rows and adds analytics
broadcast join → avoid large-side shuffle when small side fits
collect → driver memory risk
Python UDF → avoid when built-in Spark expression exists
explain → inspect plan
Spark UI → diagnose stages, shuffle, skew, memory, failures
AQE → runtime plan adaptation
```

---

<a id="ref-27-practical-engineering-rules"></a>
# Practical Engineering Rules

1. **Prefer DataFrames/Spark SQL over RDDs for production ETL.**
2. **Prefer built-in Spark functions over Python UDFs.**
3. **Filter and select only what you need.**
4. **Use Parquet/columnar formats for analytical workloads.**
5. **Understand whether an operation causes shuffle.**
6. **Broadcast only genuinely small relations.**
7. **Treat data skew as a partition-distribution problem.**
8. **Do not use `collect()` as a normal data-processing technique.**
9. **Cache only when an expensive result is reused.**
10. **Use `explain()` and Spark UI before blindly tuning configuration.**
11. **Separate execution partitioning from storage partitioning.**
12. **Design incremental pipelines to be idempotent.**
13. **Validate row counts, schema, nulls, keys, and business rules.**
14. **Make schemas explicit for important production ingestion paths.**
15. **Measure optimization changes rather than assuming they helped.**
