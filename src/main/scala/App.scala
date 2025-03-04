

import org.apache.spark.sql.SparkSession

object App {
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      println("Uso: mvn exec:java -Dexec.mainClass=\"App\" -Dexec.args=\"<capitulo> <ejercicio>\"")
      println("Ejemplo: mvn exec:java -Dexec.mainClass=\"App\" -Dexec.args=\"2 1\"")
      sys.exit(1)
    }

    val capitulo = args(0)
    val ejercicio = args(1)

    // Crear SparkSession una única vez
    val spark: SparkSession = SparkSession
      .builder
      .appName("EjerciciosScala")
      .master("local[*]")
      .getOrCreate()

    println(s"Ejecutando capítulo $capitulo, ejercicio $ejercicio")

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
      case _ => println("Capítulo no reconocido.")
    }

    // Cerrar SparkSession al finalizar
    spark.stop()
  }
}

