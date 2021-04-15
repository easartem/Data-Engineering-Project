package app

import producer.Drone
import org.apache.log4j.{Level, Logger}

object Main {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    println("Hello, world")
    val blop = new Drone(1)
    blop.generateReport().foreach(println)
    blop.sendReport(10)
  }
}
