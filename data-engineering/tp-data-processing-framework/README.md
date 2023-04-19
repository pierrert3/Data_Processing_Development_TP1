# Practices - Data engineering

## TP - Data processing with Apache Spark
To process a large amount of data partitioned on a data lake, you can use data processing frameworks such as Apache Spark :
1. Read : https://spark.apache.org/docs/latest/sql-programming-guide.html

Some questions :
* What is Spark RDD API ?
Spark RDD API (Resilient Distributed Datasets API) is the core API of Apache Spark that provides a programming interface for distributed data processing in a fault-tolerant and scalable way.
* What is Spark Dataset API ?
Spark Dataset API is a strongly-typed, immutable, and distributed collection of data that was introduced in Spark 1.6 as an improvement over the RDD API.
The Dataset API allows developers to use Spark's powerful query optimization and execution engine, called Catalyst, to optimize and execute complex queries against distributed data.
* With which languages can you use Spark ?
You can use multiple languages including Python, Scala, SQL, Java, R.
* Which data sources or data sinks can Spark work with ?
Spark can work with a wide range of data sources and data sinks, for example Apache Hadoop Distributed File System (HDFS), MongoDB, JDBC databases (such as MySQL, Oracle, and PostgreSQL), Apache Kafka.

### Analyse data with Apache Spark and Scala 
One engineering team of your company created for you a TV News data stored as JSON inside the folder `data-news-json/`.

Your goal is to analyze it with your savoir-faire, enrich it with metadata, and store it as [a column-oriented format](https://parquet.apache.org/).

1. Look at `src/main/scala/com/github/polomarcus/main/Main.scala` and update the code 

**Important note:** As you work for a top-notch software company following world-class practices, and you care about your project quality, you'll write a test for every function you write.

You can see tests inside `src/test/scala/` and run them with `sbt test`

### How can you deploy your app to a cluster of machines ?
* https://spark.apache.org/docs/latest/cluster-overview.html

### Business Intelligence (BI)
How could use we Spark to display data on a BI tool such as [Metabase](https://www.metabase.com/) ?

Tips: https://github.com/polomarcus/television-news-analyser#spin-up-1-postgres-metabase-nginxand-load-data-to-pg

### Continuous build and test
**Pro Tips** : https://www.scala-sbt.org/1.x/docs/Running.html#Continuous+build+and+test

Make a command run when one or more source files change by prefixing the command with ~. For example, in sbt shell try:
```bash
sbt
> ~ testQuick
```