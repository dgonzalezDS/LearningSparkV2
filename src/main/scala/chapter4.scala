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

    df.select("distance", "origin", "destination")
      .where("distance > 1000")
      .orderBy(col("distance").desc) // Orden descendente correctamente especificado
      .show(10)

    df.select("date", "delay", "origin", "destination")
      .where("delay > 120 AND origin = 'SFO' AND destination = 'ORD'")
      .orderBy(col("delay").desc)
      .show(10)


    df.select(
        col("delay"),
        col("origin"),
        col("destination"),
        when(col("delay") > 360, "Very Long Delays")
          .when(col("delay") > 120 && col("delay") < 360, "Long Delays")
          .when(col("delay") > 60 && col("delay") < 120, "Short Delays")
          .when(col("delay") > 0 && col("delay") < 60, "Tolerable Delays")
          .when(col("delay") === 0, "No Delays")
          .otherwise("Early")
          .alias("Flight_Delays")
      )
      .orderBy(col("origin"), col("delay").desc)
      .show(10)




  }

  def ejercicio2(spark: SparkSession): Unit = {
    /* ESTA TODO COMENTADO PARA NO ANDAR CREANDO TABLAS CONTINUAMENTE */

    /* Esta es una forma de crear una tabla gestionada */

    /*
    spark.sql("CREATE DATABASE learn_spark_db")
    spark.sql("USE learn_spark_db")
    spark.sql("CREATE TABLE managed_us_delay_flights_tbl (date STRING, delay INT, distance INT, origin STRING, destination STRING)")
    */

    /* Esta es otra*/

    /*
    val schema = StructType(Array(
      StructField("date", StringType, true),  // ✅ Ahora es STRING, no int
      StructField("delay", IntegerType, true),
      StructField("distance", IntegerType, true),
      StructField("origin", StringType, true),
      StructField("destination", StringType, true)
    ))

    val csvFile = "data/departuredelays.csv"
    val flights_df = spark.read.format("csv")
      .option("inferSchema", "false")
      .option("header", "true")
      .schema(schema)
      .load(csvFile)

    flights_df.write.saveAsTable("managed_us_delay_flights_tbl")
    */


    /* Para crear una tabla no gestionada usariamos: */
    /*

    spark.sql("""CREATE TABLE us_delay_flights_tbl(date STRING, delay INT,
     distance INT, origin STRING, destination STRING)
     USING csv OPTIONS (PATH
     'data/departuredelays.csv')""")

     /* O bien esta opción */

     /*
     (flights_df
         .write
         .option("path", "data/departuredelays.csv")
         .saveAsTable("us_delay_flights_tbl"))
     */

     */
  }
}