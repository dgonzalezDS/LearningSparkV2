import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object chapter3 {
  def ejercicio1(spark: SparkSession): Unit = {
    val dataDF = spark.createDataFrame(Seq(("Brooke", 20), ("Brooke", 25),
      ("Denny", 31), ("Jules", 30), ("TD", 35))).toDF("name", "age")
    // Group the same names together, aggregate their ages, and compute an average
    val avgDF = dataDF.groupBy("name").agg(avg("age"))
    avgDF.show()

  }
  def ejercicio2(spark: SparkSession): Unit = {

    val jsonFile= "data/blogs.json"
    val schema = StructType(Array(StructField("Id", IntegerType, false),
      StructField("First", StringType, false),
      StructField("Last", StringType, false),
      StructField("Url", StringType, false),
      StructField("Published", StringType, false),
      StructField("Hits", IntegerType, false),
      StructField("Campaigns", ArrayType(StringType), false)))

    val blogsDF = spark.read.schema(schema).json(jsonFile)
    blogsDF.show(false)
    println(blogsDF.printSchema)
    println(blogsDF.schema)


  }
  def ejercicio3 (spark:SparkSession): Unit = {
    val jsonFile= "data/blogs.json"
    val schema = StructType(Array(StructField("Id", IntegerType, false),
      StructField("First", StringType, false),
      StructField("Last", StringType, false),
      StructField("Url", StringType, false),
      StructField("Published", StringType, false),
      StructField("Hits", IntegerType, false),
      StructField("Campaigns", ArrayType(StringType), false)))

    val blogsDF = spark.read.schema(schema).json(jsonFile)
    blogsDF.columns
    blogsDF.col("Id")

    // Multiplicar una columna por 2
    blogsDF.select(expr("Hits * 2")).show(2) // Elegir este o el de abajo, ambos hacen lo mismo
    blogsDF.select(col("Hits")*2).show(2)

    // Crear una nueva columna, segun una expresion condicional
    blogsDF.withColumn("Big Hitters", (expr("Hits > 10000"))).show()

    // Concatenar 3 columnas y crear y mostrar la columna resultante

    blogsDF
      .withColumn("AuthorsID", (concat(expr("First"), expr("Last"), expr("Id"))))
      .select(col("AuthorsID"))
      .show(4)

    // Ordenar en orden descendente por "Id"

    blogsDF.sort(col("Id").desc).show()
   // blogsDF.sort($"Id".desc).show() // Este no me funciona

  }

  def ejercicio4(spark:SparkSession): Unit = {
    val fireSchema = StructType(Array(
      StructField("CallNumber", IntegerType, true),
      StructField("UnitID", StringType, true),
      StructField("IncidentNumber", IntegerType, true),
      StructField("CallType", StringType, true),
      StructField("CallDate", StringType, true),
      StructField("WatchDate", StringType, true),
      StructField("CallFinalDisposition", StringType, true),
      StructField("AvailableDtTm", StringType, true),
      StructField("Address", StringType, true),
      StructField("City", StringType, true),
      StructField("Zipcode", IntegerType, true),
      StructField("Battalion", StringType, true),
      StructField("StationArea", StringType, true),
      StructField("Box", StringType, true),
      StructField("OriginalPriority", StringType, true),
      StructField("Priority", StringType, true),
      StructField("FinalPriority", IntegerType, true),
      StructField("ALSUnit", BooleanType, true),
      StructField("CallTypeGroup", StringType, true),
      StructField("NumAlarms", IntegerType, true),
      StructField("UnitType", StringType, true),
      StructField("UnitSequenceInCallDispatch", IntegerType, true),
      StructField("FirePreventionDistrict", StringType, true),
      StructField("SupervisorDistrict", StringType, true),
      StructField("Neighborhood", StringType, true),
      StructField("Location", StringType, true),
      StructField("RowID", StringType, true),
      StructField("Delay", FloatType, true)
    ))
    val sfFireFile= "data/sf-fire-calls.csv"
    val fireDF = spark.read.schema(fireSchema)
      .option("header", "true")
      .csv(sfFireFile)

    fireDF.show(10)

    val parquetPath = "data/fireDF"
    fireDF.write.format("parquet").save(parquetPath)
  }

  def ejercicio5(spark:SparkSession): Unit = {

    val fireSchema = StructType(Array(
      StructField("CallNumber", IntegerType, true),
      StructField("UnitID", StringType, true),
      StructField("IncidentNumber", IntegerType, true),
      StructField("CallType", StringType, true),
      StructField("CallDate", StringType, true),
      StructField("WatchDate", StringType, true),
      StructField("CallFinalDisposition", StringType, true),
      StructField("AvailableDtTm", StringType, true),
      StructField("Address", StringType, true),
      StructField("City", StringType, true),
      StructField("Zipcode", IntegerType, true),
      StructField("Battalion", StringType, true),
      StructField("StationArea", StringType, true),
      StructField("Box", StringType, true),
      StructField("OriginalPriority", StringType, true),
      StructField("Priority", StringType, true),
      StructField("FinalPriority", IntegerType, true),
      StructField("ALSUnit", BooleanType, true),
      StructField("CallTypeGroup", StringType, true),
      StructField("NumAlarms", IntegerType, true),
      StructField("UnitType", StringType, true),
      StructField("UnitSequenceInCallDispatch", IntegerType, true),
      StructField("FirePreventionDistrict", StringType, true),
      StructField("SupervisorDistrict", StringType, true),
      StructField("Neighborhood", StringType, true),
      StructField("Location", StringType, true),
      StructField("RowID", StringType, true),
      StructField("Delay", FloatType, true)
    ))
    val sfFireFile= "data/sf-fire-calls.csv"
    val fireDF = spark.read.schema(fireSchema)
      .option("header", "true")
      .csv(sfFireFile)

    val fewFireDF = fireDF
      .select("IncidentNumber", "AvailableDtTm", "CallType")
      .where(col("CallType") =!= "Medical Incident")
    fewFireDF.show(5, false)

    fireDF
      .select("CallType")
      .where(col("CallType").isNotNull)
      .agg(countDistinct("CallType") as "DistinctCallTypes")
      .show()

    fireDF
      .select("CallType")
      .where(col("CallType").isNotNull)
      .distinct()
      .show(10, false)

  }
}