import Tasks._
import models._
import org.apache.spark.sql._

object Main {
  def main(args: Array[String]): Unit = {
    println("Creating Spark context...")
    val st = new Storage()

    import st.spark.implicits._

    val passengers = st.loadCsv[passenger]("passengers.csv", Encoders.product[passenger].schema)
    val flightData = st.loadCsv[flight]("flightData.csv", Encoders.product[flight].schema)

    val myTasks = new Tasks(flightData, passengers)

    // output
    println(myTasks.task1.show())
    println(myTasks.task2.show(100))
    println(myTasks.task3.show())
    println(myTasks.task4(4).show())
    println(myTasks.task5.show())

    println("Stopping Spark context...")
    st.stop
  }
}