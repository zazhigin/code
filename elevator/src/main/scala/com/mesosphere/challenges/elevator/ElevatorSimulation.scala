package com.mesosphere.challenges.elevator

import scala.collection.mutable

/**
  * Created by zazhigin on 29.07.16.
  */
object ElevatorSimulation {

  val etc = new DefaultElevatorControlSystem

  def start(): Unit = {
    for (line <- scala.io.Source.stdin.getLines()) {
      val opts = line.split("\\s+")
      val command = opts(0)
      command match {
        case "status" => status(opts)
        case "update" => update(opts)
        case "pickup" => pickup(opts)
        case "step"   => step(opts)
        case _ => throw new Exception(s"Wrong command '$command'")
      }
    }
  }

  def status(opts: Array[String]): Unit = {
    val comment = opts(1)
    println(s"Status $comment:")
    etc.status foreach {
      case(elevatorId, elevator) =>
        println(s"  Elevator ID ($elevatorId): $elevator")
    }
  }

  def update(opts: Array[String]): Unit = {
    val elevatorId :: floorNumber :: goalFloorNumbers =
      opts.tail.toList.map(_.toInt)
    etc.update(elevatorId,
      new Elevator(floorNumber, goalFloorNumbers.to[mutable.SortedSet]))
  }

  def pickup(opts: Array[String]): Unit = {
    val pickupFloor = opts(1).toInt
    val direction = opts(2).toInt
    etc.pickup(new Pickup(pickupFloor, direction))
  }

  def step(opts: Array[String]): Unit = {
    val n = if (opts.length > 1) opts(1).toInt else 1
    (1 to n) foreach (_ => etc.step())
  }

  def main(args: Array[String]) = {
    start()
  }
}
