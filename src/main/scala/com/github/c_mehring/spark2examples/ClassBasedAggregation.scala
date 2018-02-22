package com.github.c_mehring.spark2examples

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.DoubleType

case class SalesRecord(transactionId: String, customerId: String, itemId: String, amountPaid: Double)

object ClassBasedAggregation {

  val sparkSession = SparkSession.builder.
    master("local")
    .appName("aggregation example")
    .getOrCreate()

  import sparkSession.implicits._

  def main(args: Array[String]): Unit = {
    loadSales().
      groupByKey(_.itemId).
      mapGroups((groupId, groupRecords) => {
        // the original case class should be used for further processing
        // we want to compress the existing data
        groupRecords.reduce((r1, r2) => new SalesRecord(groupId, "", r1.itemId, r1.amountPaid + r2.amountPaid))
      }).
      show
  }


  def loadSales() = {
    sparkSession.read
      .option("header", "true")
      .csv("src/main/resources/sales.csv")
      .withColumn("amountPaid", 'amountPaid.cast(DoubleType))
      .as[SalesRecord]
  }
}
