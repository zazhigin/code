package com.mesosphere.challenges.elevator

/**
  * Created by zazhigin on 29.07.16.
  */
class Pickup(val pickupFloor: Int,
             val direction: Int) {

  override def toString: String = s"Pickup Floor = $pickupFloor, Direction = $direction"
}
