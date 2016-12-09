import com.mesosphere.challenges.elevator._
import org.scalatest._

import scala.collection.mutable

/**
  * Created by zazhigin on 02.08.16.
  */
class ElevatorTest extends FlatSpec with Matchers {

  "Elevator" should "pickup event in ascending order" in {
    val elevator = new Elevator(3, mutable.SortedSet())

    elevator.pickup(new Pickup(5, 2))
    elevator.pickup(new Pickup(7, 4))
    assert(elevator.goalFloorNumbers.size == 3)
    assert(elevator.goalFloorNumbers.toList(1) == 7)

    elevator.pickup(new Pickup(3, 0))
    assert(elevator.goalFloorNumbers.size == 4)

    elevator.pickup(new Pickup(4, 1))
    elevator.pickup(new Pickup(5, 2))
    elevator.pickup(new Pickup(6, 3))
    assert(elevator.goalFloorNumbers.size == 7)
    assert(elevator.goalFloorNumbers.toList(2) == 5)

    elevator.pickup(new Pickup(7, 4))
    assert(elevator.goalFloorNumbers.size == 7)

    elevator.pickup(new Pickup(8, 5))
    assert(elevator.goalFloorNumbers.size == 9)
    assert(elevator.goalFloorNumbers.toList(4) == 7)
  }
  it should "pickup event in descending order" in {
    val elevator = new Elevator(10, mutable.SortedSet())
    elevator.pickup(new Pickup(5, -4))
    elevator.pickup(new Pickup(7, -3))
    assert(elevator.goalFloorNumbers.size == 4)
    assert(elevator.goalFloorNumbers.toList(1) == 4)
  }
  it should "pickup event in descending order 2" in {
    val elevator = new Elevator(8, mutable.SortedSet())

    elevator.pickup(new Pickup(8, -1))
    assert(elevator.goalFloorNumbers.size == 1)

    elevator.pickup(new Pickup(3, -1))
    assert(elevator.goalFloorNumbers.size == 3)
    assert(elevator.goalFloorNumbers.head == 2)
    assert(elevator.goalFloorNumbers.toList(2) == 7)
  }
}
