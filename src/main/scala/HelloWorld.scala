import org.apache.spark.sql.SparkSession

/**
  * Hello world (word count).
  */
object HelloWorld extends App {

  // Get spark session
  val spark = SparkSession
    .builder()
    .master(System.getenv().getOrDefault("MASTER", "yarn"))
    .appName("hello-world")
    .getOrCreate()

  val phrases = spark.sparkContext.parallelize(Seq("Hello world"))

  val counts = phrases.flatMap(l => l.split(" ")).map(w => (w, 1)).reduceByKey(_ + _)

  // Print result
  counts.collect.foreach {
    case (w: String, c: Int) => println(s"$w -> $c times")
  }

  spark.stop()

}
