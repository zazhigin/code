package com.mesosphere.challenges.elevator

/**
  * Created by zazhigin on 29.07.16.
  */
trait ElevatorControlSystem {

  /**
    * Gets status of all elevators.
    *
    * @return map of elevator states
    */
  def status: Map[Int, Elevator]

  /**
    * Gets list of all pickup events.
    *
    * @return list of pickup events
    */
  def pickupEvents: List[Pickup]

  /**
    * Updates elevator state by elevator id.
    *
    * @param elevatorId The elevator id.
    * @param elevator The updated elevator state.
    */
  def update(elevatorId: Int, elevator: Elevator)

  /**
    * Picking up event.
    *
    * @param event The picking up event with move info.
    */
  def pickup(event: Pickup)

  /**
    * Simulation step.
    */
  def step()
}
