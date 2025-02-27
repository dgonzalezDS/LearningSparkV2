import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.ml.source.image
import org.apache.spark.sql.SaveMode
import scala.util.Random

object chapter7 {
  def ejercicio1(spark: SparkSession): Unit = {
    /*
    En este ejercicio creamos unos dataframes grandes para experimentar con los join de spark, empaquetando (bucketing)
    y guardando en formato parquet
     */

    import spark.implicits._
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

    // Generate some sample data for two data sets

    var states = scala.collection.mutable.Map[Int, String]()
    var items = scala.collection.mutable.Map[Int, String]()
    val rnd = new scala.util.Random(42)

    // Initialize states and items purchased

    states += (0 -> "AZ", 1 -> "CO", 2-> "CA", 3-> "TX", 4 -> "NY", 5-> "MI")
    items += (0 -> "SKU-0", 1 -> "SKU-1", 2-> "SKU-2", 3-> "SKU-3", 4 -> "SKU-4",
      5-> "SKU-5")

    // Create DataFrames
    val usersDF = (0 to 1000000).map(id => (id, s"user_${id}",
        s"user_${id}@databricks.com", states(rnd.nextInt(5))))
      .toDF("uid", "login", "email", "user_state")
    val ordersDF = (0 to 1000000)
      .map(r => (r, r, rnd.nextInt(10000), 10 * r* 0.2d,
        states(rnd.nextInt(5)), items(rnd.nextInt(5))))
      .toDF("transaction_id", "quantity", "users_id", "amount", "state", "items")

    // Do the join

    val usersOrdersDF = ordersDF.join(usersDF, $"users_id" === $"uid")

    // Show the joined results

    usersOrdersDF.show(false)


    // Save as managed tables by bucketing them in Parquet format

    usersDF.orderBy(asc("uid"))
      .write.format("parquet")
      .bucketBy(8, "uid")
      .mode(SaveMode.Overwrite)
      .saveAsTable("UsersTbl")
    ordersDF.orderBy(asc("users_id"))
      .write.format("parquet")
      .bucketBy(8, "users_id")
      .mode(SaveMode.Overwrite)
      .saveAsTable("OrdersTbl")

    // Cache the tables

    spark.sql("CACHE TABLE UsersTbl")
    spark.sql("CACHE TABLE OrdersTbl")

    // Read them back in

    val usersBucketDF = spark.table("UsersTbl")
    val ordersBucketDF = spark.table("OrdersTbl")

    // Do the join and show the results

    val joinUsersOrdersBucketDF = ordersBucketDF
      .join(usersBucketDF, $"users_id" === $"uid")
    joinUsersOrdersBucketDF.show(false)


  }
}