package com.github.c_mehring.spark2examples

import org.apache.spark.sql.{Column, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._


object DataAndLogColumn {

  val initFieldLogDataUDF = udf(() => Seq[FieldLogData]())

  def main(args: Array[String]) {

    val sparkSession = SparkSession.builder.
      master("local")
      .appName("complex cells")
      .getOrCreate()

    import sparkSession.implicits._

    // records as loaded from database
    val rawData = sparkSession.createDataset(Seq(1, 2, 3, 4, 5)).select(col("value").as("data"))


    // split columns into orginal col and list of issues (_#log)
    val preparedDataset = rawData.select(createLogDataColumns(rawData): _*)

    val additions = Seq(10, 20)

    // append item to array
    val logDS = preparedDataset.withColumn("data#log", runDqOperationsOnColumn(additions)(col("data"), col("data#log")))

    logDS.show

    logDS.printSchema

    logDS.explain
  }

  def createLogDataColumns(ds: Dataset[Row]): Array[Column] = {
    ds.schema.fields.flatMap(f => {
      // add compagnon column with logdata for each input column
      Array(col(f.name), initFieldLogDataUDF().as(f.name + "#log"))
    })
  }

  def runDqOperationsOnColumn(ops: Seq[Int]) = udf((columnValue: AnyRef, currentLog: Seq[FieldLogData]) => {

    val dqResult = ops
      .map(add => Option(new FieldLogData(add, s"hello $add", "", s"$add")))
      .filter(_.isDefined)
      .map(_.get)

    // concat old messages and new messages
    currentLog ++ dqResult

  })
}

case class FieldLogData(level: Int, msg: String, modelRef: String, dataRef: String)
