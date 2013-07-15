
import spark._
import spark.scheduler.{StageCompleted, StatsReportListener}
import spark.streaming._
import spark.SparkContext._
import spark.streaming.StreamingContext._
import spark.streaming.dstream.ConstantInputDStream

object DriverScaleTest {
  def main (args: Array[String]) {

    println(System.getProperty("hello"))
    if (args.size < 4) {
      println(this.getClass.getSimpleName + " <Spark home dir>  <Spark driver URL>  <batch size in milliseconds>  <tasks per batch>")
      System.exit(1)
    }

    val sparkHome = args(0)
    val sparkDriverURL = args(1)
    val batchSize = args(2).toInt
    val tasksPerBatch = args(3).toInt

    /*
    val sc = new SparkContext(sparkDriverURL, "DriverScaleTest", sparkHome, 
      List("target/scala-2.9.3/driver-scale-test_2.9.3-1.0.jar")) 
    sc.makeRDD(1 to 100, 10).collect().foreach(println)
    sc.stop()
    */

    val add = (x: Int, y: Int) => x + y

    val ssc = new StreamingContext(sparkDriverURL , "DriverScaleTest", Milliseconds(batchSize), sparkHome,
      List("target/scala-2.9.3/driver-scale-test_2.9.3-1.0.jar"))
    ssc.sparkContext.addSparkListener(new StatsReportListener)

    val inputStream = new ConstantInputDStream(ssc, ssc.sparkContext.makeRDD(1 to tasksPerBatch, tasksPerBatch))
    inputStream.map(x => (x, 1)).register()
    ssc.start()
    /*Thread.sleep(30000)*/
    /*ssc.stop()*/
    /*System.exit(0)*/
  }
}






