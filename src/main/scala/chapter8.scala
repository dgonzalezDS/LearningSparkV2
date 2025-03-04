import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.ml.source.image
import org.apache.spark.sql.streaming._

object chapter8 {
  def ejercicio1(spark: SparkSession): Unit = {
    /*
    PDTE DE ARREGLAR
     */

    val lines = spark
      .readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()

    val query = lines.writeStream
      .format("console")
      .outputMode("append")
      .start()

    query.awaitTermination()



  }
}