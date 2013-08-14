
import spark._
import spark.scheduler.{StageCompleted, StatsReportListener}
import spark.streaming._
import spark.SparkContext._
import spark.streaming.StreamingContext._
import spark.streaming.dstream.ConstantInputDStream

object DriverScaleTest {

  val offload = true 
  val offloaders = 9
  val coresPerOffloader = 8

  def main (args: Array[String]) {

    System.setProperty("spark.streaming.concurrentJobs", if (offload) "100" else "1")
    System.setProperty("spark.offload.initCores", coresPerOffloader.toString)
    System.setProperty("spark.streaming.offloaders", if (offload) offloaders.toString else "0")

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

    /*ssc.sparkContext.addSparkListener(new StatsReportListener)*/

    val rddGenerator = (sc: SparkContext) => Some(sc.makeRDD(1 to tasksPerBatch, tasksPerBatch))
    val inputStream = new spark.streaming.dstream.RDDGeneratorInputDStream(ssc, rddGenerator)
    inputStream.map(x => (x, 1)).foreach(rdd => {
        if (offload) rdd.sparkContext.runOffloadedJob[(Int, Int), Unit](rdd, (iter: Iterator[(Int, Int)]) => { })
        else rdd.sparkContext.runJob[(Int, Int), Unit](rdd, (iter: Iterator[(Int, Int)]) => { })
      })

    var batchesCompleted = 0
    ssc.addStreamingListener(new StreamingListener {
        def onBatchCompletion(batchTime: Time) {
          println("Finished batch of time " + batchTime)
          batchesCompleted += 1
        }
      })

    /*val inputStream = new ConstantInputDStream(ssc, ssc.sparkContext.makeRDD(1 to tasksPerBatch, tasksPerBatch))*/
    /*inputStream.map(x => (x, 1)).register()*/
    Thread.sleep(1000)
    ssc.start()
    /*Thread.sleep(batchSize * 100)*/
    /*assert(batchesCompleted > 2)*/
    /*println("" + batchesCompleted + " batches completed")*/
    /*ssc.stop()*/
    /*Thread.sleep(30000)*/
    /*ssc.stop()*/
    /*System.exit(0)*/
  }
}






