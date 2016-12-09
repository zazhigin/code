package com.mesosphere.challenges.elevator

import scala.collection.mutable

/**
  * Created by zazhigin on 29.07.16.
  */
class Elevator(var floorNumber: Int,
               val goalFloorNumbers: mutable.SortedSet[Int]) {

  def getDirection: Int = {
    if (goalFloorNumbers.isEmpty) return 0
    val direction = goalFloorNumbers.head - floorNumber
    direction
  }

  def pickup(event: Pickup): Unit = {
    val sign = Integer.signum(event.direction)
    if ((event.pickupFloor - floorNumber) * sign > 0)
      goalFloorNumbers += event.pickupFloor
    goalFloorNumbers += event.pickupFloor + event.direction
  }

  def step(): Unit = {
    if (goalFloorNumbers.isEmpty) return
    val direction = getDirection
    if (direction > 0) floorNumber += 1
    if (direction < 0) floorNumber -= 1
    goalFloorNumbers -= floorNumber
  }

  override def toString: String =
    s"Floor Number = $floorNumber, Goal Floor Numbers = $goalFloorNumbers"
}
