import models.{genericModel}
import org.apache.spark.sql._
import org.apache.spark.sql.types.StructType

class Storage {
  val spark = SparkSession.builder()
    .master("local[1]")
    .appName("Flight_Data")
    .getOrCreate()

  def loadCsv[T <: genericModel : Encoder](path: String, shm: StructType): Dataset[T] = {
    spark.read.format("csv").option("header", "true").schema(shm).csv(path).as[T]
  }

  def stop = {
    spark.stop()
  }
}
