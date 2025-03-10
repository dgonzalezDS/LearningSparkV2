import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._

object chapter2 {
  def ejercicio1()(implicit spark: SparkSession): Unit = {
    /*
    Este ejercicio de los MnM consiste en cargar un archivo csv que contiene datos de colores y estados
    Una vez cargado el csv y asignado a un data frame 'mnmDF', primero hacemos un conteo y una agrupacion por colores y estados
    Para al final hacer un sumatorio total de colores para el estado de california
     */
    val archivoCSV = "data/mnm_dataset.csv" // Se define la ruta del CSV aquí
    println(s"Cargando datos desde: $archivoCSV")

    val mnmDF: DataFrame = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(archivoCSV)

    val countMnMDF = mnmDF
      .select("State", "Color", "Count")
      .groupBy("State", "Color")
      .agg(count("Count").alias("Total"))
      .orderBy(desc("Total"))

    countMnMDF.show(60)
    println(s"Total Rows = ${countMnMDF.count()}")

    val caCountMnMDF = mnmDF
      .select("State", "Color", "Count")
      .where(col("State") === "CA")
      .groupBy("State", "Color")
      .agg(count("Count").alias("Total"))
      .orderBy(desc("Total"))

    caCountMnMDF.show(10)
  }
}
