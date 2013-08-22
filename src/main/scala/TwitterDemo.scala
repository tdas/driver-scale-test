import spark.streaming.{Minutes, Seconds, StreamingContext}
import StreamingContext._
import spark.SparkContext._
import TwitterDemoHelper._

object TwitterDemo {
  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: TwitterDemo <master> <checkpoint HDFS path>")
      System.exit(1)
    }

    val master = args(0)
    val checkpointPath = "./checkpoint/"

    // Create the StreamingContext
    val ssc = new StreamingContext(master, "TwitterDemo", Seconds(1))
    ssc.checkpoint(checkpointPath)

    // Create the streams of tweets
    val tweets = ssc.twitterStream()

    // Count the tags over a 1 minute window
    val tagCounts = tweets.flatMap(status => getTags(status))
                          .countByValueAndWindow(Minutes(1), Seconds(1))

    // Sort the tags by counts
    val sortedTags = tagCounts.map { case (tag, count) => (count, tag) }
                              .transform(_.sortByKey(false))

    // Print top 10 tags
    sortedTags.foreach(showTopTags(20) _)

    ssc.start()
  }
}
