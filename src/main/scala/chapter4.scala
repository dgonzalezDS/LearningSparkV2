import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object chapter4 {
  def ejercicio1(spark: SparkSession): Unit = {

    val schema = StructType(Array(
      StructField("date", StringType, true),  // ✅ Ahora es STRING, no int
      StructField("delay", IntegerType, true),
      StructField("distance", IntegerType, true),
      StructField("origin", StringType, true),
      StructField("destination", StringType, true)
    ))

    val csvFile = "data/departuredelays.csv"
    val df = spark.read.format("csv")
      .option("inferSchema", "false")
      .option("header", "true")
      .schema(schema)
      .load(csvFile)

    df.createOrReplaceTempView("us_delay_flights_tbl")

    spark.sql(
      """ SELECT distance, origin, destination
        FROM us_delay_flights_tbl WHERE distance >1000
        ORDER BY distance DESC
        """).show(10)
    spark.sql(
      """SELECT date, delay, origin, destination
        FROM us_delay_flights_tbl
        WHERE delay > 120 AND ORIGIN = 'SFO' AND DESTINATION = 'ORD'
        ORDER BY delay DESC """)

    val dfFormatted = df.withColumn(
      "date_formatted",
      to_timestamp(col("date"), "MMddHHmm")
    )

    dfFormatted.show(10, false) // Mostrar sin truncar

    spark.sql(
      """SELECT delay, origin, destination,
        CASE
            WHEN delay > 360 THEN 'Very Long Delays'
            WHEN delay > 120 AND delay < 360 THEN 'Long Delays'
            WHEN delay > 60 AND delay < 120 THEN 'Short Delays'
            WHEN delay > 0 and delay < 60 THEN 'Tolerable Delays'
            WHEN delay = 0 THEN 'No Delays'
            ELSE 'Early'
        END AS Flight_Delays
        FROM us_delay_flights_tbl
        ORDER BY origin, delay DESC
        """).show(10)

  }
}