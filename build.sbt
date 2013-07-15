name := "Driver Scale Test"

version := "1.0"

scalaVersion := "2.9.3"

libraryDependencies += "org.spark-project" % "spark-streaming_2.9.3" % "0.7.3"

libraryDependencies += "org.spark-project" % "spark-core_2.9.3" % "0.7.3"

resolvers ++= Seq(
    "Akka Repository" at "http://repo.akka.io/releases/",
    "Spray Repository" at "http://repo.spray.cc/"
  )

fork in run := true

javaOptions in run += "-Dhello=kitty -XX:+PrintGC -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.port=9000 -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.authenticate=false -Djava.rmi.server.hostname=aws-ds-cp-dev-tathagad-6001.iad6.amazon.com "


