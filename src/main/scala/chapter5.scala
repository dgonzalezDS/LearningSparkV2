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
  def ejercicio1(spark: SparkSession): Unit = {
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

  def ejercicio2 (spark: SparkSession): Unit = {

    val url = "jdbc:sqlserver://localhost:1433;databaseName=AdventureWorks2008R2;encrypt=false;trustServerCertificate=false;integratedSecurity=true;"
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

  def ejercicio3 (spark: SparkSession): Unit = {
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


}
