

import org.apache.spark.sql.SparkSession

/*

Para ejecutar un ejecicio concreto, es necesario pasar como parametros de ejecucion el número del capitulo y el número de ejercicio
Por ejemplo, si queremos ejecutar el ejercicio 2 del capitulo 3, deberemos pasar  3 2  como parametros de ejecucion

Tambien es posible ejecutar los ejercicios desde la terminal. Para ello, es necesario usar los comandos mvn exec, como se especifica al inicio del ejercicio
 */

object App {
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      println("Uso: mvn exec:java -Dexec.mainClass=\"App\" -Dexec.args=\"<capitulo> <ejercicio>\"")
      println("Ejemplo: mvn exec:java -Dexec.mainClass=\"App\" -Dexec.args=\"2 1\"")
      sys.exit(1)
    }

    val capitulo = args(0)
    val ejercicio = args(1)

    // Crear SparkSession una única vez para utilizarlo en cada capitulo/ejercicio
    implicit val spark: SparkSession = SparkSession
      .builder
      .appName("EjerciciosScala")
      .master("local[*]")
      .config("spark.executor.memory", "4g")
      .config("spark.driver.memory", "4g")
      .getOrCreate()

    println(s"Ejecutando capítulo $capitulo, ejercicio $ejercicio")

    val chapterNumber = args(0)
    val exerciseNumber = args(1)

    /* NOTA: En caso de querer usar reflexion y no match-case
    val clazz = Class.forName(s"chapter$chapterNumber$$")
    val module = clazz.getField("MODULE$").get(null)
    val methodName = s"ejercicio$exerciseNumber"
    val exerciseMethod = clazz.getMethod(methodName, classOf[SparkSession])
    exerciseMethod.invoke(module,spark) "
    */

    (chapterNumber, exerciseNumber) match {
      case ("2", "1") => chapter2.ejercicio1()
      case ("3", "1") => chapter3.ejercicio1()
      case ("3", "2") => chapter3.ejercicio2()
      case ("3", "3") => chapter3.ejercicio3()
      case ("3", "4") => chapter3.ejercicio4()
      case ("3", "5") => chapter3.ejercicio5()
      case ("4", "1") => chapter4.ejercicio1()
      case ("4", "2") => chapter4.ejercicio2()
      case ("5", "1") => chapter5.ejercicio1()
      case ("5", "2") => chapter5.ejercicio2()
      case ("5", "3") => chapter5.ejercicio3()
      case ("5", "4") => chapter5.ejercicio4()
      case ("5", "5") => chapter5.ejercicio5()
      case ("6", "1") => chapter6.ejercicio1()
      case ("6", "2") => chapter6.ejercicio2()
      case ("7", "1") => chapter7.ejercicio1()

      case _ => println("Capítulo o ejercicio no encontrado")
    }
    // Cerrar SparkSession al finalizar
    spark.stop()
  }
}

