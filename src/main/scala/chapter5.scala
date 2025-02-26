import org.apache.spark.sql.{SparkSession, DataFrame}

/*
*   Este tema trata sobre la conexion de fuentes de datos externas, como bases de datos de Postgre o MS SQL
*   Para ejecutar correctamente el capitulo, además de pasar por parámetros de ejecución el numero de capitulo y el ejercicio,
*   es necesario pasar por "variables de entorno"  las credenciales de acceso a las diferentes bases de datos, ocultas por motivos de seguridad.
*
*   Nota: Cada conexión es distinta, pero en la mayoría (o todas) es necesario especificar el nombre de la base de datos y el nombre de la tabla que queremos conectar
*  */
probando cosas
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
      .jdbc(url, "customers", properties)

    // Mostrar los datos
    df.show()
  }


}
