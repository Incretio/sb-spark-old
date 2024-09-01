import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}


object data_mart {

  val password = ""

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .config("spark.cassandra.connection.host", "10.0.0.31")
      .config("spark.cassandra.connection.port", "9042")
      .getOrCreate()

    val clients: DataFrame = loadFromCassandra(spark)
      .filter(col("age") >= 18)
      .withColumn("age_cat",
        when(col("age").between(18, 24), "18-24")
          .when(col("age").between(25, 34), "25-34")
          .when(col("age").between(35, 44), "35-44")
          .when(col("age").between(45, 54), "45-54")
          .otherwise(">=55"))
      .drop("age")

    val shopCategory: DataFrame = loadFromElasticSearch(spark)
      .filter(col("uid").isNotNull)
      .select("uid", "category")
      .withColumn("category", concat(lit("shop_"), lower(regexp_replace(col("category"), "[ -]", "_"))))
      .groupBy("uid").pivot("category").agg(count("uid")).na.fill(0)

    val logs: DataFrame = loadFromHdfs(spark)
      .withColumn("domain", explode(col("visits.url")))
      .drop("visits")
      .withColumn("domain", regexp_replace(regexp_replace(col("domain"), "^(http://)|(https://)", ""), "^www[.]", ""))
      .withColumn("domain", split(col("domain"), "/").getItem(0))

    val webCategory: DataFrame = loadFromPg(spark).join(logs, "domain")
      .drop("domain")
      .withColumn("category", concat(lit("web_"), lower(regexp_replace(col("category"), "[ -]", "_"))))
      .groupBy("uid").pivot("category").agg(count("uid")).na.fill(0)


    val result = clients
      .join(shopCategory, "uid", "left_outer").na.fill(0)
      .join(webCategory, "uid", "left_outer").na.fill(0)
    result.limit(10).show()

    writeToPg(result, "clients")
  }

  def loadFromCassandra(spark: SparkSession): DataFrame = {
    spark.read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "clients", "keyspace" -> "labdata"))
      .load()
  }

  def loadFromElasticSearch(spark: SparkSession): DataFrame = {
    spark.read
      .format("org.elasticsearch.spark.sql")
      .options(Map("es.read.metadata" -> "true",
        "es.nodes.wan.only" -> "true",
        "es.port" -> "9200",
        "es.nodes" -> "10.0.0.31",
        "es.net.ssl" -> "false"))
      .load("visits")
  }

  def loadFromHdfs(spark: SparkSession): DataFrame = {
    spark.read
      .json("hdfs:///labs/laba03/weblogs.json")
  }

  def loadFromPg(spark: SparkSession): DataFrame = {
    spark.read
      .format("jdbc")
      .option("url", "jdbc:postgresql://10.0.0.31:5432/labdata")
      .option("dbtable", "domain_cats")
      .option("user", "oleg_rodin")
      .option("password", password)
      .option("driver", "org.postgresql.Driver")
      .load()
  }

  def writeToPg(data: DataFrame, tableName: String): Unit = {
    data.write
      .format("jdbc")
      .option("url", "jdbc:postgresql://10.0.0.31:5432/oleg_rodin")
      .option("dbtable", tableName)
      .option("user", "oleg_rodin")
      .option("password", password)
      .option("driver", "org.postgresql.Driver")
      .option("truncate", value = true)
      .mode("overwrite")
      .save()
  }

}