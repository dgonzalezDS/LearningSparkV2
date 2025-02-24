import org.apache.spark.sql.{SparkSession, DataFrame}



object chapter5 {
  def ejercicio1(spark: SparkSession): Unit = {


    // Parámetros de conexión
    val url = "jdbc:postgresql://localhost:5432/OLTP"
    val properties = new java.util.Properties()

    // Obtener usuario y contraseña de manera segura
    val user = sys.env.getOrElse("POSTGRES_USER", "default_user")
    val password = sys.env.getOrElse("POSTGRES_PASSWORD", "default_password")

    properties.setProperty("user", user)
    properties.setProperty("password", password)
    properties.setProperty("driver", "org.postgresql.Driver")

    // Leer datos de PostgreSQL en un DataFrame
    val df = spark.read
      .jdbc(url, "customers", properties)

    // Mostrar los datos
    df.show()
  }



}
