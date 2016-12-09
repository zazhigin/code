import com.mesosphere.challenges.elevator._
import org.scalatest._

import scala.collection.mutable

/**
  * Created by zazhigin on 02.08.16.
  */
class DefaultElevatorControlSystemTest extends FlatSpec with Matchers {

  "DefaultElevatorControlSystemTest" should "get proper closest elevator" in {
    val ecs = new DefaultElevatorControlSystem
    ecs.update(111, new Elevator(3, mutable.SortedSet()))
    ecs.update(222, new Elevator(4, mutable.SortedSet()))
    ecs.update(333, new Elevator(10, mutable.SortedSet()))
    assert(ecs.getClosestElevator(new Pickup(9, -2))._1 == 333)
    assert(ecs.getClosestElevator(new Pickup(5, 1))._1 == 222)
    assert(ecs.getClosestElevator(new Pickup(3, -1))._1 == 111)
  }
  it should "update elevator states by pickup events" in {
    val ecs = new DefaultElevatorControlSystem
    ecs.update(111, new Elevator(3, mutable.SortedSet()))
    ecs.update(222, new Elevator(4, mutable.SortedSet()))
    ecs.update(333, new Elevator(5, mutable.SortedSet()))
    ecs.pickup(new Pickup(6, 2))
    ecs.pickup(new Pickup(4, 3))
    ecs.pickup(new Pickup(3, 6))
    ecs.updateElevatorsByPickupEvents()
    assert(ecs.pickupEvents.isEmpty)
    ecs.pickup(new Pickup(4, -1))
    ecs.updateElevatorsByPickupEvents()
    assert(ecs.pickupEvents.size == 1)
  }
  it should "update elevator states by step" in {
    val ecs = new DefaultElevatorControlSystem
    ecs.update(111, new Elevator(3, mutable.SortedSet()))
    ecs.update(222, new Elevator(4, mutable.SortedSet()))
    ecs.update(333, new Elevator(5, mutable.SortedSet()))
    ecs.pickup(new Pickup(6, 2))
    ecs.pickup(new Pickup(4, 3))
    ecs.pickup(new Pickup(3, 6))
    ecs.updateElevatorsByPickupEvents()
    assert(ecs.pickupEvents.isEmpty)
    ecs.pickup(new Pickup(4, -1))
    ecs.step()
    ecs.step()
    assert(ecs.pickupEvents.size == 1)
    ecs.step()
    ecs.updateElevatorsByPickupEvents()
    assert(ecs.pickupEvents.isEmpty)
  }
}
