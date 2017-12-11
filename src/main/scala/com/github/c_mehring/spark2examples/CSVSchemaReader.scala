package com.github.c_mehring.spark2examples

import org.apache.spark.sql.SparkSession

object CSVSchemaReader {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder.
      master("local")
      .appName("DQ with UDF example")
      .getOrCreate()


    val df = sparkSession.read
      .option("header", "true")
      .csv("src/main/resources/customers.csv")
      // type conversions
      .selectExpr("cast(customerId as int) customerId", "customerName")

    df.printSchema()

    df.show()
  }


}
