import akka.actor.{ActorSystem, ActorRef, Props, Actor}
import akka.pattern.ask
import akka.dispatch.{Future, Await}
import akka.util.duration._

import spark._
import spark.SparkContext._
import spark.streaming.RegisterReceiver

import scala.collection.mutable.{ArrayBuffer, HashSet}
import spark.util.Distribution
import spark.Logging

object AkkaTest {

  case class Register()

  case class Ping(
      var creationTime: Long = System.currentTimeMillis(),
      var workerReceiveTime: Long = 0,
      var masterReceiveTime: Long = 0) {
  }

  case class Test()

  case class Metrics(totalTime: Long, sendingTime: Long, receivingTime: Long)
  {
    def this(ping: Ping) = this(
      ping.masterReceiveTime - ping.creationTime,
      ping.workerReceiveTime - ping.creationTime,
      ping.masterReceiveTime - ping.workerReceiveTime
    )
  }


  class DriverActor extends Actor {

    def receive = {
      case test: Test => {
        println("Driver Actor tested")
      }

      case Register() => {
        if (workerActors.contains(sender)) {
          throw new Exception("Duplicate registration for " + sender)
        }
        val workerActor = sender
        //context.actorFor("akka://spark@%s:%s/user/%s".format(workerHostname, workerPort, workerName))
        workerActors += workerActor
        //sender ! true
        println("Registered " + sender.path)
      }

      case (creationTime: Long, workerReceiveTime: Long) => {
        val masterReceiveTime = System.currentTimeMillis()
        //println("Received ping back from worker " + sender)
        val metrics = new Metrics(
          masterReceiveTime - creationTime,
          workerReceiveTime - creationTime,
          masterReceiveTime - workerReceiveTime
        )
        //println("metrics = " + metrics)
        allMetrics += metrics
      }
    }
  }



  class WorkerActor(actorSystem: ActorSystem, driverActorPath: String) extends Actor {
    val driverActor = actorSystem.actorFor(driverActorPath)
    val timeout = 5.seconds

    override def preStart() {
      driverActor ! Register()
      println("Registered worker actor to driver")
    }

    def receive = {
      case test: Test => {
        println("Worker actor tested")
      }

      case (creationTime: Long, array: Array[Byte]) => {
        //println("Received ping from driver " + sender + " along with an array of " + array.size + " bytes ")
        driverActor ! (creationTime, System.currentTimeMillis())
      }

      case z: Any => {
        println("Received something [" + z + "] from " + sender)
      }
    }
  }


  val workerActors = new HashSet[ActorRef]
  val allMetrics = new ArrayBuffer[Metrics]

  def setup(sc: SparkContext, numWorkers: Int) {
    println("SETTING UP")
    val env = SparkEnv.get
    val actorSystem = env.actorSystem
    val driverActor = actorSystem.actorOf(Props(new DriverActor), "AkkaTestDriverActor")
    println("Driver actor setup at " + driverActor.path)
    driverActor ! Test()

    sc.makeRDD(1 to numWorkers, numWorkers).foreach(i => {
      val actorSystem = SparkEnv.get.actorSystem
      val ip = System.getProperty("spark.driver.host", "localhost")
      val port = System.getProperty("spark.driver.port", "7077").toInt
      val driverActorPath = "akka://spark@%s:%s/user/AkkaTestDriverActor".format(ip, port)

      val workerActor = actorSystem.actorOf(
        Props(new WorkerActor(actorSystem, driverActorPath)),
        "AkkaTestWorkerActor-" + i
      )
      println("Worker actor setup at " + workerActor.path)
    })
    Thread.sleep(1000)
  }

  def test(numMessages: Int, numIterations: Int, messageSize: Int) {
    println("TESTING")
    val numWorkers = workerActors.size

    val numMessagesPerWorker =
      (1 to numMessages).grouped( math.ceil(numMessages / numWorkers.toDouble).toInt ).map(_.size).toSeq

    for (i <- 1 to numIterations) {
      println("Starting iteration " + i)
      allMetrics.clear()
      println("Pinging " + workerActors.size + " workers")

      val startTime = System.currentTimeMillis()
      val array = new Array[Byte](messageSize)

      workerActors.zip(numMessagesPerWorker).foreach {
        case (workerActor, numMessagesToSend) => {
          (1 to numMessagesToSend).foreach( i => workerActor ! (System.currentTimeMillis(),array) )
          //println("Sent " + numMessagesToSend + " messages sent to " + workerActor)
        }
      }

      println("Waiting for " + workerActors.size + " metrics to return")
      while(allMetrics.size < numMessages) {
        Thread.sleep(10)
        println("Got " + allMetrics.size + " metrics")
      }
      val stopTime = System.currentTimeMillis()

      println(("-" * 20) + " Iteration " + i + ("-" * 20))
      println("Total round-trip times")
      Distribution(allMetrics.map(_.totalTime.toDouble)).foreach(_.summary())
      println("Sending times")
      Distribution(allMetrics.map(_.sendingTime.toDouble)).foreach(_.summary())
      println("Receiving times")
      Distribution(allMetrics.map(_.receivingTime.toDouble)).foreach(_.summary())
      println("Total iteration time")
      Distribution(Seq(stopTime - startTime).map(_.toDouble)).foreach(_.summary())
    }
    println("Done " + numIterations)
  }

  def doFullTest(args: Array[String]) {
    println(System.getProperty("hello"))
    if (args.size < 5) {
      println(this.getClass.getSimpleName + " <Spark home dir>  <Spark driver URL>  <# workers>  <# iterations> <# messages / iteration> [# bytes / message]")
      System.exit(1)
    }

    val sparkHome = args(0)
    val sparkDriverURL = args(1)
    val numWorkers = args(2).toInt
    val numIterations = args(3).toInt
    val numMessages = args(4).toInt
    val messageSize = if (args.size > 5) args(5).toInt else 1

    val sc = new SparkContext(sparkDriverURL, "AkkaTest", sparkHome,
      List("target/scala-2.9.3/driver-scale-test_2.9.3-1.0.jar"))

    setup(sc, numWorkers)
    //warmup(100)
    test(numMessages, numIterations, messageSize)

    println("# workers = " + numWorkers)
    println("# iterations = " + numIterations)
    println("# messages / iteration = " + numMessages)
    println("# bytes per message = " + messageSize)
  }


  def doUnitTest() {
    val system = ActorSystem("spark")

    println("Started actor system")

    val driverActor = system.actorOf(Props[DriverActor], "AkkaTestDriverActor")
    println("Set up driver actor")

    driverActor ! Test()
    println("Messaged driver actor")

    val workerActor = system.actorOf(Props(new WorkerActor(system, driverActor.path.toString )), "AkkaTestWorkerActor-1")
    println("Set up worker actor")
    Thread.sleep(1000)
    assert(workerActors.size == 1)

    workerActor ! Test()
    println("Messaged worker actor directly")
    Thread.sleep(1000)

    workerActors.head ! Test()
    println("Messaged worker actor through sender ref " + workerActors.head.path)
    Thread.sleep(1000)

    test(10, 10, 1)

    Thread.sleep(5000)
    println("Shutting down")
    system.shutdown()

  }


  def main(args: Array[String]) {
    doFullTest(args)
  }



}
