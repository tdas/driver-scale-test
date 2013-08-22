import spark.streaming._
import spark.streaming.StreamingContext._
import spark.storage.StorageLevel
import twitter4j._
import spark.RDD

object TwitterDemoHelper {

  def getTags(status: Status) = status.getText.split(" ").filter(_.startsWith("#"))

  def showTopTags(k: Int)(rdd: RDD[(Long, String)], time: Time) {
    val topTags = rdd.take(k)
    val topTagsString =  topTags.map(x => "\tTag: " + x._2.formatted("%-40s") + "\t Freq: " + x._1).mkString("\n")
    println()
    println("Popular tags in last 60 seconds (at " + time + ")\n" + topTagsString)
    println()

    /*val topTagsTable = "<table>\n"+ topTags.map(x => "<td>" + x._2 + "</td><td>" + x._1 + "</td>").mkString("<tr>", "</tr><tr>", "</tr>") + "\n</table>"*/
    /*StreamingDashboard.updateContents("Popular tags in last 60 seconds", Seq(topTagsTable))*/
  }
}



