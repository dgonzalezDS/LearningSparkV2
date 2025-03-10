import org.apache.spark.sql.{SparkSession, DataFrame}
import java.util.Properties
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import scala.util.Random._

object chapter6 {
  /*
 En este capitulo crearemos case class para un conjunto de datos, la crearemos fuera de los ejercicios, para poder
 usar la case class en todos los metodos
  */
  case class Bloggers(Id:Long, First:String, Last:String, Url:String, Published:String, Hits:Long, Campaigns:Array[String])
  case class Usage(uid:Int, uname:String, usage: Int)
  case class UsageCost(uid: Int, uname:String, usage: Int, cost: Double)

  def ejercicio1()(implicit spark: SparkSession): Unit = {
    /*
    Leemos el archivo bloggers.json usando nuestra case class
    */
    import spark.implicits._

    val bloggers = "data/blogs.json"
    val bloggersDS = spark
      .read
      .format("json")
      .option("path", bloggers)
      .load()
      .as[Bloggers]

    bloggersDS.show(10)
  }

  def ejercicio2()(implicit spark: SparkSession): Unit = {
    /*
    Creamos un case class y generamos data random para almacenarla en el dataset
     */
    import spark.implicits._

    val r = new scala.util.Random(42)
    // Create 1000 instances of scala Usage class
    // This generates data on the fly
    val data = for (i <- 0 to 1000)
      yield (Usage(i, "user-" + r.alphanumeric.take(5).mkString(""),
        r.nextInt(1000)))
    // Create a Dataset of Usage typed data
    val dsUsage = spark.createDataset(data)
    dsUsage.show(10)

    dsUsage
      .filter(d => d.usage > 900)
      .orderBy(desc("usage"))
      .show(5, false)

    def computeUserCostUsage(u: Usage): UsageCost = {
      val v = if (u.usage > 750) u.usage * 0.15 else u.usage * 0.50
      UsageCost(u.uid, u.uname, u.usage, v)
    }
    dsUsage.map(u => {computeUserCostUsage(u)}).show(5)
  }
}