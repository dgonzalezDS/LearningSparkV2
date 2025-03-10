import org.apache.spark.sql.{SparkSession, DataFrame}
import java.util.Properties
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._



/*
*   Este tema trata sobre la conexion de fuentes de datos externas, como bases de datos de Postgre o MS SQL
*   Para ejecutar correctamente el capitulo, además de pasar por parámetros de ejecución el numero de capitulo y el ejercicio,
*   es necesario pasar por "variables de entorno"  las credenciales de acceso a las diferentes bases de datos, ocultas por motivos de seguridad.
*
*   Nota: Cada conexión es distinta, pero en la mayoría (o todas) es necesario especificar el nombre de la base de datos y el nombre de la tabla que queremos conectar
*  */

object chapter5 {
  def ejercicio1()(implicit spark: SparkSession): Unit = {
    /* Conexion con POSTGRESDB */

    // Parámetros de conexión
    val url = "jdbc:postgresql://localhost:5432/OLTP"  // El nombre de la base de datos es OLTP (postgre usa por defecto el puerto 5432)
    val properties = new java.util.Properties()

    // Obtener usuario y contraseña de manera segura
    val user = sys.env.getOrElse("POSTGRES_USER", "default_user")
    val password = sys.env.getOrElse("POSTGRES_PASSWORD", "default_password")

    properties.setProperty("user", user)
    properties.setProperty("password", password)
    properties.setProperty("driver", "org.postgresql.Driver")

    // Leer datos de PostgreSQL en un DataFrame
    val df = spark.read
      .jdbc(url, "customers", properties)  // El nombre de la tabla dentro de la base de datos "OLTP" es "customers"

    // Mostrar los datos
    df.show()
  }

  def ejercicio2 ()(implicit spark: SparkSession): Unit = {
    /*
    Conexion con MS SQL Server
     */


    val url = "jdbc:sqlserver://L2203030\\SQLEXPRESS:1433;databaseName=AdventureWorks2008R2;encrypt=false;trustServerCertificate=false;integratedSecurity=true;"
    val properties = new Properties()
    properties.setProperty("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")

    // Leer datos desde la tabla "customers"
    val df = spark.read.format("jdbc")
        .option("url", url)
        .option("dbtable", "Person.Person")
        .option ("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
        .load()
    df.show(10)


  }

  def ejercicio3 ()(implicit spark: SparkSession): Unit = {
    import spark.implicits._

    /* En este ejercicio creamos  un data frame a partir de 2 arrays, con el objetivo de aplicarle funciones de mayor nivel, como transform (), filter (), exists(),
      reduce ()
     */

    // Create DataFrame with two rows of two arrays (tempc1, tempc2)
    val t1 = Array(35, 36, 32, 30, 40, 42, 38)
    val t2 = Array(31, 32, 34, 55, 56)
    val tC = Seq(t1, t2).toDF("celsius")
    tC.createOrReplaceTempView("tC")
    // Show the DataFrame
    tC.show()



    /* Funcion transform () */
    println("\n===== USANDO SPARK.SQL =====\n")

    spark.sql("""
    SELECT celsius,
    transform(celsius, t -> ((t * 9) div 5) + 32) as fahrenheit
    FROM tC
    """).show()

    /* Funcion filter () */
    spark.sql("""
    SELECT celsius,
    filter(celsius, t -> t > 38) as high
    FROM tC
    """).show()

    /* Funcion exists () */

    spark.sql("""
    SELECT celsius,
    exists(celsius, t -> t = 38) as threshold
    FROM tC
    """).show()



    /* Funcion reduce () NOTA: Está funcion no está incluida en mi version de Spark */

    /*

    spark.sql("""
    SELECT celsius,
    reduce(
    celsius,
     0,
     (t, acc) -> t + acc,
     acc -> (acc div size(celsius) * 9 div 5) + 32
     ) as avgFahrenheit
     FROM tC
     """).show()

     */

    /* EXTRA: Ahora reescribimos el codigo anterior, para usar los metodos de dataframe en lugar de spark.sql  */

    println("\n===== USANDO METODOS DE DF =====\n")


    /* Función transform () */
    val dfTransform = tC.select(
      col("celsius"),
      transform(col("celsius"), t => (t * 9) / 5 + 32).as("fahrenheit")
    )
    dfTransform.show()

    /* Función filter () */
    val dfFilter = tC.select(
      col("celsius"),
      filter(col("celsius"), t => t > 38).as("high")
    )
    dfFilter.show()

    /* Función exists () */
    val dfExists = tC.select(
      col("celsius"),
      exists(col("celsius"), t => t === 38).as("threshold")
    )
    dfExists.show()




  }

  def ejercicio4 ()(implicit spark: SparkSession): Unit = {
  /*

   En este ejercicio cargaremos dos archivos de datos de vuelos de EE.UU, haremos una conversion de tipos en dos de las columnas y crearemos una tabla mas pequeña
   con la que trabajar mas facilmente, ya que las tablas originales tienen 1.2M de registros

   */

    // Ruta de los archivos a importar

    val delaysPath =
      "data/departuredelays.csv"
    val airportsPath =
      "data/airport-codes-na.txt"

    // Cargamos el data set de los aeropuertos y creamos una vista temporal

    val airports = spark.read
      .option("header","true")
      .option("inferSchema", "true")
      .option("delimiter", "\t")
      .csv(airportsPath)
    airports.createOrReplaceTempView("airports_na")

    // Cargamos el data set de los retrasos (departuredelays) y creamos uan vista temporal

    val delays = spark.read
      .option("header","true")
      .csv(delaysPath)
      .withColumn("delay", expr("CAST(delay as INT) as delay"))
      .withColumn("distance", expr("CAST(distance as INT) as distance"))
    delays.createOrReplaceTempView("departureDelays")

    // Creamos una tabla temporal mas pequeña llamada foo

    val foo = delays.filter(
      expr(
        """origin == 'SEA' AND destination == 'SFO' AND
            date like '01010%' AND delay > 0 """))
    foo.createOrReplaceTempView("foo")

    // Mostramos la tabla pequeña

    spark.sql("SELECT * FROM foo").show()

    // Con nuestra tabla lista, procedemos a hacer diferentes operaciones

    // UNION
    /*
    El dataframe creado, bar, es la union de foo con delays,por tanto hemos añadido registros ya existentes adelays, y si filtramos los resultados,
    veremos que hay registros duplicados
     */
    val bar = delays.union(foo)
    bar.createOrReplaceTempView("bar")
    bar.filter(expr("""origin == 'SEA' AND destination == 'SFO' AND date LIKE '01010%' AND delay > 0""")).show()

    // JOIN

    /*
    Por defecto, al hacer un JOIN, Spark realiza una inner join, de entre las opciones posibles que son:
    outer, full, full_outer, left, left_outer, right, right_outer, left_semi y left_anti
     */

    foo.join(
      airports.as('air),
      col("air.IATA") === col("origin")
    ).select("City", "State", "date", "delay", "distance", "destination","origin").show()


    // AÑADIR COLUMNAS

    val foo2 = foo.withColumn(
      "status",
      expr("CASE WHEN delay <= 10 THEN 'On-time' ELSE 'Delayed' END")
    )
    foo2.show()

    // ELIMINAR COLUMNAS

    val foo3 = foo2.drop("delay")
    foo3.show()

    // RENOMBRAR COLUMNAS

    val foo4 = foo3.withColumnRenamed("status", "flight_status")
    foo4.show()






  }


  /* EJERCICIO DE WINDOWING */

  // Creamos un Dataframe sencillo a mano

  def ejercicio5 ()(implicit spark: SparkSession): Unit = {
    import spark.implicits._

    val df_1 = Seq(("800", "BMW",8000),("110", "Bugatti", 8000),("208", "Peugot", 5400),("Atlas", "Volkswagen", 5000), ("Mustang", "Ford", 5000), ("C500", "Mercedes", 5000),
      ("Prius", "Toyota", 3200), ("Landcruiser", "Toyota", 3000), ("Accord", "Honda", 2000), ("C200", "Mercedes", 2000), ("Corolla","Toyota", 1800)
    ).toDF("name","company","power")

    df_1.show()
    df_1.createOrReplaceTempView("df_1")

    spark.sql(
      """
        SELECT name, company, power, Rank
        FROM (
            SELECT name, company, power,
                   RANK() OVER (ORDER BY power DESC) AS Rank
            FROM df_1
        )
      """
    ).show()

    spark.sql(
      """
        SELECT name, company, power, Rank
        FROM (
            SELECT name, company, power,
                   DENSE_RANK() OVER (ORDER BY power DESC) AS Rank
            FROM df_1
        )
      """
    ).show()

    spark.sql(
      """
        SELECT name, company, power, Rank
        FROM (
            SELECT name, company, power,
                   ROW_NUMBER() OVER (ORDER BY power DESC) AS Rank
            FROM df_1
        )
      """
    ).show()

  }





}
