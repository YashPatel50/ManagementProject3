import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object FilterTransactions {
  def main(args: Array[String]) {
    val spark = SparkSession.builder()
      .appName("FilterTransactions")
      .master("local[*]")
      .getOrCreate()

    val filePath = "hdfs://localhost:9000/user/Project1/data/transactions"

    println("Reading in transactions from:", filePath)

    val transactionsDf = spark.read
      .option("header", false)
      .option("inferSchema", true)
      .csv(filePath)
      .withColumnRenamed("_c0", "transactionID")
      .withColumnRenamed("_c1", "customerID")
      .withColumnRenamed("_c2", "transactionTotal")
      .withColumnRenamed("_c3", "numberItems")
      .withColumnRenamed("_c4", "transactionDesc")

    println("Finished reading transactions")

    //println("---------T1---------------")
    val T1 = transactionsDf.filter("transactionTotal > 200")
    //T1.show()


    println("---------T2---------------")
    val T2 = T1.groupBy("numberItems")
      .agg(
      sum("transactionTotal").as("sum_total"),
      avg("transactionTotal").as("avg_total"),
      min("transactionTotal").as("min_total"),
      max("transactionTotal").as("max_total"))
    T2.show()

    //println("---------T3---------------")
    val T3 = T1.groupBy("customerID").count().as("transactionsMade")
    //T3.show()

    //println("---------T4---------------")
    val T4 = transactionsDf.filter("transactionTotal > 600")
    //T4.show()

    //println("---------T5---------------")
    val T5 = T4.groupBy("customerID").count().as("transactionsMade")
    //T5.show()

    println("---------T6---------------")
    val T6 = T3.join(T5, "customerID").where(T5("count") * 5 < T3("count")).select("customerID")
    T6.show()

    spark.stop()
  }
}