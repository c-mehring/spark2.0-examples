package com.github.c_mehring.spark2examples

import org.apache.spark.sql.SparkSession

object SimpleAggregation {

  val sparkSession = SparkSession.builder.
    master("local")
    .appName("aggregation example")
    .getOrCreate()

  def main(args: Array[String]): Unit = {

    import org.apache.spark.sql.functions._

    // sum of payments per itemId
    loadSales()
      .orderBy("itemId")
      .groupBy("itemId")
      .agg(
        count("itemId").as("numOfSales"),
        sum("amountPaid").as("turnOverPerMonth"))
      .show()

    // customer with max turnover, needs to be improvementﬂ
    val row = loadSales()
      .groupBy("customerId")
      .agg(
        sum("amountPaid").as("turnOverPerMonth"),
        avg("amountPaid").as("avgAmount")
      )
      .orderBy("turnOverPerMonth")
      .first()
    println(row.schema)
    println(row)

  }

  def loadSales() = sparkSession.read
    .option("header", "true")
    .csv("src/main/resources/sales.csv")


}
