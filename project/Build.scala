import sbt._
import Keys._

object MyBuild extends Build {

  val Mklauncher = config("mklauncher") extend(Compile)
  val mklauncher = TaskKey[Unit]("mklauncher")
  val mklauncherTask = mklauncher <<= (target, fullClasspath in Runtime) map { (target, cp) =>
    def writeFile(file: File, str: String) {
      val writer = new java.io.PrintWriter(file)
      writer.println(str)
      writer.close()
    }
    val cpString = cp.map(_.data).mkString(":")
    val launchString = """
    CLASSPATH="%s"
    scala -usejavacp -Djava.class.path="${CLASSPATH}" "$@"
    """.format(cpString)
    val targetFile = (target / "scala-sbt").asFile
    writeFile(targetFile, launchString)
    targetFile.setExecutable(true)
  }


  lazy val project = Project (
    "",
    file ("."),
    settings = Defaults.defaultSettings ++ Seq(mklauncherTask)
  )
}


