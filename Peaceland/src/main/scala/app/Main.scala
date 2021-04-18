package app

import producer.Drone
import org.apache.log4j.{Level, Logger}

import java.util.{Timer, TimerTask}

object Main extends App {
    Logger.getLogger("org").setLevel(Level.OFF)
    println("Hello, world")
    val blop = new Drone(1)
    //blop.simulateDrone()
    blop.simulateDrone(1)


    /** Event scheduler task test
    val taskEvent = new TimerTask {
        override def run(): Unit = {
            println("New event")
        }
    }
    val timerEvent = new Timer()
    timerEvent.schedule(taskEvent, 0, 3000) **/
}
