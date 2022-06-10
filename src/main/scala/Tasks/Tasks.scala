package Tasks

import models._
import org.apache.spark.sql._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._
import udfs.udfs._

import java.sql.Date

class Tasks(val flightData: Dataset[flight], val passengers: Dataset[passenger]) {

  val task1 = flightData.withColumn("Month", month(col("date")))
    .select(col("flightId"), col("Month"))
    .distinct()
    .groupBy("Month").count()
    .withColumnRenamed("count", "Number of Flights")
    .orderBy(col("Month"))

  val task2 = flightData
    .groupBy("passengerId").count()
    .join(passengers, "passengerId")
    .withColumnRenamed("passengerId", "Passenger ID")
    .withColumnRenamed("count", "Number of Flights")
    .withColumnRenamed("firstName", "First name")
    .withColumnRenamed("lastName", "Last name")
    .orderBy(desc("Number of Flights"))

  val task3 = {
    val removeAdjacentUDF = udf(removeAdjacent)
    val w = Window.partitionBy("passengerId").orderBy("flightId")
    val flightDataCleansed =
      flightData
        .withColumn("trip_item", array("from", "to"))
        .withColumn("trips_unflattened", collect_list("trip_item").over(w))
        .groupBy("passengerId")
        .agg(max("trips_unflattened").as("trips_unflattened"))
        .withColumn("trips", removeAdjacentUDF(flatten(col("trips_unflattened"))))
        .drop("trips_unflattened")

    flightDataCleansed.withColumn("split", split(array_join(col("trips"), " "), "uk"))
      .withColumn("filter", filter(col("split"), (col: Column) => col =!= "" || col =!= null))
      .withColumn("n_trip", array_max(transform(col("filter"), (col: Column) => size(split(trim(col), " ")))))
      .withColumn("n_trip", when(col("n_trip").isNull, 0).otherwise(col("n_trip")))
      .drop("split", "filter", "trips")
      .withColumnRenamed("passengerId", "Passenger ID")
      .withColumnRenamed("n_trip", "Longest Run")
  }

  val task4 = (atLeastNTimes: Int) => {
    val minMaxDate = flightData
      .agg(min("date"), max("date"))
      .first()

    val minDate = minMaxDate(0).asInstanceOf[java.sql.Date]
    val maxDate = minMaxDate(1).asInstanceOf[java.sql.Date]
    flownTogether(atLeastNTimes, minDate, maxDate)
      .drop("From", "To")
  }

  val task5 = {
    val minDate = Date.valueOf("2017-01-01")
    val maxDate = Date.valueOf("2017-05-01")
    flownTogether(10, minDate, maxDate)
  }

  def flownTogether(atLeastNTimes: Int, from: java.sql.Date, to: java.sql.Date) = {
    val flightDataTruncated = flightData.filter(col("date") >= from && col("date") <= to)
    val passenger1 = flightDataTruncated.select(col("passengerId"), col("flightId"), col("date"))
      .withColumnRenamed("passengerId", "passenger1")
    val passenger2 = flightDataTruncated.select("passengerId", "flightId")
      .withColumnRenamed("passengerId", "passenger2")

    passenger1.join(passenger2, "flightId")
      .filter("passenger1<passenger2")
      .groupBy("passenger1", "passenger2")
      .agg(count(lit(1)).alias("Number of flights together"),
        min("date").alias("From"),
        max("date").alias("To"))
      .filter(col("Number of flights together") >= atLeastNTimes)
      .withColumnRenamed("passenger1", "Passenger 1 ID")
      .withColumnRenamed("passenger2", "Passenger 2 ID")
  }
}
