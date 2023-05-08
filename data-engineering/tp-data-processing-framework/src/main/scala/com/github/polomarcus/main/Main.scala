package com.github.polomarcus.main

import com.github.polomarcus.model.News
import com.typesafe.scalalogging.Logger
import com.github.polomarcus.utils.{ClimateService, NewsService, SparkService}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}


object Main {
  def main(args: Array[String]) {
    val logger = Logger(this.getClass)
    logger.info("Used `sbt run` to start the app")

    // This is our Spark starting point
    // Open file "src/main/scala/utils/SparkService.scala"
    // Read more about it here : https://spark.apache.org/docs/latest/sql-getting-started.html#starting-point-sparksession
    val spark = SparkService.getAndConfigureSparkSession()
    import spark.implicits._

    // Read a JSON data source with the path "./data-news-json"
    // Tips : https://spark.apache.org/docs/latest/sql-data-sources-json.html
    val pathToJsonData = "./data-news-json/"
    val newsDataframe: DataFrame =  spark.read.json(pathToJsonData)

    // To type our dataframe as News, we can use the Dataset API : https://spark.apache.org/docs/latest/sql-getting-started.html#creating-datasets
    val newsDatasets: Dataset[News] = NewsService.read(pathToJsonData)

    // print the dataset schema - tips : https://spark.apache.org/docs/latest/sql-getting-started.html#untyped-dataset-operations-aka-dataframe-operations
    newsDatasets.printSchema()

    // Show the first 10 elements - tips : https://spark.apache.org/docs/latest/sql-getting-started.html#creating-dataframes
    newsDatasets.show(10)

    // Enrich the dataset by apply the ClimateService.isClimateRelated function to the title and the description of a news
    // a assign this value to the "containsWordGlobalWarming" attribute
    val enrichedDataset = NewsService.enrichNewsWithClimateMetadata(newsDatasets)
    //enrichedDataset.show(50)

    // From now, we'll use only the Dataset API as it's more convenient
    val filteredNewsAboutClimate = NewsService.filterNews(enrichedDataset) // only news that are talking about climate
    filteredNewsAboutClimate.show()
    // Count how many tv news we have in our data source
    val count = NewsService.getNumberOfNews(filteredNewsAboutClimate)
    logger.info(s"We have ${count} news in our dataset")


    // Show how many news we have talking about climate change compare to others news (not related climate)
    // Tips: use a groupBy
    enrichedDataset.groupBy("containsWordGlobalWarming").count().show() // it add all the news that are climate related and the news not climate related

    val totalNews = NewsService.getNumberOfNews(newsDatasets)
    logger.info(s"We have talked ${count} times of climate change in a total of ${totalNews} news")

    // Use SQL to query a "news" table - look at : https://spark.apache.org/docs/latest/sql-getting-started.html#running-sql-queries-programmatically
    /** basic SQL queries with a DataFrame that is not strongly typed -> use of DataFrame API's */
    newsDataframe.createOrReplaceTempView("news") // Create a temporary view of the filteredNewsAboutClimate dataset as a "news" table
    val BasicSqlQuery = spark.sql("SELECT * FROM news WHERE containsWordGlobalWarming = true") // run the SQL query
    BasicSqlQuery.show()

    // Use strongly typed dataset to be sure to not introduce a typo (faute de frappe) to your SQL Query
    // Tips : https://stackoverflow.com/a/46514327/3535853
    /** strongly types data set allows to be sure that there is no typo in the sql query and to be sure that column names
     * and data types are correct and consistent with the data structure (we benefit from autocompletion and data type
     * checking during compilation). -> use of Dataset API's */
    newsDatasets.createOrReplaceTempView("news") // Create a temporary view of the filteredNewsAboutClimate dataset as a "news" table
    val StronglyTypedSqlQuery = spark.sql("SELECT * FROM news WHERE containsWordGlobalWarming = true") // run the SQL query
    StronglyTypedSqlQuery.show()


    // Save it as a columnar format with Parquet with a partition by date and media
    // Learn about Parquet : https://spark.apache.org/docs/3.2.1/sql-data-sources-parquet.html
    // Learn about partition : https://spark.apache.org/docs/3.2.1/sql-data-sources-load-save-functions.html#bucketing-sorting-and-partitioning

    /** enregistrement des données dans in fichier parquet */
    /** partition des données: division des données en groupes plus petits basés sur la data et le média
     * -> creatio de répertoires basés sur les valeurs des colonnes "date" et "média". Cela permet de regrouper les données liées à
     * une date spécifique ou à un média spécifique dans des répertoires séparés, ce qui peut améliorer les performances
     * de requête et de traitement des données. */
    StronglyTypedSqlQuery.write
      .partitionBy("date", "media")
      .parquet("./climateRelatedNews.parquet")

    //newsDatasets.info("Stopping the app")
    System.exit(0)
  }
}
