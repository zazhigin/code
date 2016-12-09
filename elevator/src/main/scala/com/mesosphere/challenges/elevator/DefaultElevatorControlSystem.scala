package com.mesosphere.challenges.elevator

import scala.collection.mutable

/**
  * Created by zazhigin on 29.07.16.
  */
class DefaultElevatorControlSystem() extends ElevatorControlSystem {

  val elevators = mutable.HashMap[Int, Elevator]()
  val events = mutable.Queue[Pickup]()

  /**
    * Gets status of all elevators.
    *
    * @return map of elevator states
    */
  override def status: Map[Int, Elevator] = elevators.toMap

  /**
    * Gets list of all pickup events.
    *
    * @return list of pickup events
    */
  override def pickupEvents: List[Pickup] = events.toList

  /**
    * Updates elevator state by elevator id.
    *
    * @param elevatorId The elevator id.
    * @param elevator The updated elevator state.
    */
  override def update(elevatorId: Int, elevator: Elevator): Unit = {
    elevators.update(elevatorId, elevator)
  }

  /**
    * Picking up event.
    *
    * @param event The picking up event with move info.
    */
  override def pickup(event: Pickup): Unit = {
    events.enqueue(event)
  }

  /**
    * Simulation step.
    */
  override def step(): Unit = {
    updateElevatorsByPickupEvents()
    updateElevatorsByStep()
    // update elevators after simulation step
    updateElevatorsByPickupEvents()
  }

  def updateElevatorsByPickupEvents(): Unit = {
    val n = events.size
    (1 to n) foreach {_ =>
      val event = events.head
      val best = getClosestElevator(event)
      if (best._2 != null) {
        best._2.pickup(event)
        events.dequeue()
      }
    }
  }

  def getClosestElevator(event: Pickup): (Int, Elevator) = {
    val sign = Integer.signum(event.direction)

    // filtering idle or same/reverse direction elevators
    val f = elevators filter {
      case(elevatorId, elevator) => elevator.getDirection == 0 ||
        elevator.getDirection * sign > 0 &&
          (event.pickupFloor - elevator.floorNumber) * sign >= 0
    }
    if (f.isEmpty) return (0, null)

    // calculate deltas for each filtered elevator
    val c = for (i <- f) yield
      (Math.abs(event.pickupFloor - i._2.floorNumber), i)

    // min by sorting elevators by deltas and getting first
    val m = c.toSeq.sortBy(_._1).head
    m._2
  }

  def updateElevatorsByStep(): Unit = {
    elevators.foreach {
      case(elevatorId, elevator) => elevator.step()
    }
  }
}
