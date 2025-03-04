

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
    val spark: SparkSession = SparkSession
      .builder
      .appName("EjerciciosScala")
      .master("local[*]")
      .getOrCreate()

    println(s"Ejecutando capítulo $capitulo, ejercicio $ejercicio")

    val chapterNumber = args(0)
    val exerciseNumber = args(1)

    val clazz = Class.forName(s"chapter$chapterNumber$$")
    val module = clazz.getField("MODULE$").get(null)
    val methodName = s"ejercicio$exerciseNumber"
    val exerciseMethod = clazz.getMethod(methodName, classOf[SparkSession])
    exerciseMethod.invoke(module, spark)

    // El CODIGO SIGUIENTE ERA LA FORMA ORIGINAL DE HACERLO, PERO POCO EFICIENTE, LA VERSION ACTUAL ES EL CODIGO DE ARRIBA

    /*
    capitulo match {
      case "2" =>
        ejercicio match {
          case "1" => chapter2.ejercicio1(spark)
          case _   => println("Ejercicio no encontrado en el capítulo 2.")
        }

      case "3" =>
        ejercicio match {
          case "1" => chapter3.ejercicio1(spark)
          case "2" => chapter3.ejercicio2(spark)
          case "3" => chapter3.ejercicio3(spark)
          case "4" => chapter3.ejercicio4(spark)
          case "5" => chapter3.ejercicio5(spark)
          case _   => println("Ejercicio no encontrado en el capítulo 3.")
        }
      case "4" =>
        ejercicio match {
          case "1" => chapter4.ejercicio1(spark)
          case "2" => chapter4.ejercicio2(spark)
          case "3" => chapter4.ejercicio3(spark)
        }
      case "5" =>
        ejercicio match {
          case "1" => chapter5.ejercicio1(spark)
          case "2" => chapter5.ejercicio2(spark)
          case "3" => chapter5.ejercicio3(spark)
          case "4" => chapter5.ejercicio4(spark)
          case "5" => chapter5.ejercicio5(spark)
        }
      case "6" =>
        ejercicio match {
          case "1" => chapter5.ejercicio1(spark)
        }
      case "7" =>
        ejercicio match {
          case "1" => chapter7.ejercicio1(spark)
        }
      case "8" =>
        ejercicio match {
          case "1" => chapter8.ejercicio1(spark)
        }
      case _ => println("Capítulo no reconocido.")
    }
*/
    // Cerrar SparkSession al finalizar
    spark.stop()
  }
}

