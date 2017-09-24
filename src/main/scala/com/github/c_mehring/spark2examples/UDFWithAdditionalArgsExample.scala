package com.github.c_mehring.spark2examples

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * DQ Validation with UDF example
  */
object UDFWithAdditionalArgsExample {

  def main(args: Array[String]) {

    val sparkSession = SparkSession.builder.
      master("local")
      .appName("DQ with UDF example")
      .getOrCreate()

    val df = sparkSession.read.option("header", "true").csv("src/main/resources/sales_dqproblems.csv")
      // init VALID flag as unchecked '-1'
      .withColumn("VALID", lit(-1))

    // add null validation for each col
    val dfWithValidFlag = df
      // new column showing udf with additional args
      .withColumn("REPLACE",
      replaceColumnValues(Map(("113", 999)))(col("transactionId")))
      // validation aggregation to VALID per row
      .withColumn("VALID",
      greatest(col("VALID"), // override initial state '0'
        nullCheckUDF(col("transactionId")),
        nullCheckUDF(col("customerId"))))

    dfWithValidFlag.show()
    dfWithValidFlag.explain(true)
  }

  // nonsense replacement function, use first argument list to supply context for spark udf-function def
  // derived from https://gist.github.com/andrearota/5910b5c5ac65845f23856b2415474c38#file-example-scala
  def replaceColumnValues(replacementLookup: Map[String, Double]) = udf((newValue: String) => {
    replacementLookup.get(newValue) match {
      case Some(originalValue) => originalValue
      case None => 0D
    }
  })

  private def nullCheckUDF = udf(nullCheckFunc)

  private def nullCheckFunc = (input: AnyRef) => Option(input) match {
    case None => {
      // do something with logging
      // return
      1
    }
    case _ => 0
  }


}
