Mesosphere Distributed Applications Engineer Challenge
======================================================

Implementation of an elevator control system.

Solution
--------

I implemented improved version of ElevatorControlSystem interface which has list of Goal Floor Numbers instead of single goal.
This helped me to find optimal minimum implementation of elevator control system.

My algorithm is most optimal way to handle pickup requests (PickupEvent) and much more optimal than FCFS.
In every step algorithm is choosing closest elevator on the same elevator direction (or idle elevator).

Only in one exclusive case when all elevators following in opposite direction pickup event queue is using.
And at the moment when some elevator change direction (or become idle) pickup event list is being processed and cleaned.


Requirements
------------

1. `Scala 2.11`
2. `Java 1.7`
3. `SBT`


Building elevator
-----------------

```
$ git clone https://github.com/zazhigin/code.git
$ cd code/elevator
$ sbt compile
```


Execution elevator
------------------

```
$ sbt run < data/test1.txt
```


Input and output
----------------

Input of elevator is managed by redirecting via standard input testing scenario.
Output of elevator with status of elevator control system displayed by standard output.

You can redirect standard output to text file by:
```
$ sbt run < data/test3.txt > test3.out
```


Test scenario format
--------------------

Elevator control system is declared by interface 'ElevatorControlSystem' and implemented by DefaultElevatorControlSystem class.
For testing convenience wrapper ElevatorSimulation class was implemented.
This class is reading test scenario from standard input.

Each line represent one command with arguments.
Here is the list of all available commands:

1. Printing status command: `status <Comment>` (comment is to identife this status in the output)
2. Updating elevator state by command: `update <Elevator ID> <FloorNumber> [<Goal FloorNumber>]+`
3. Picking up event by command: `pickup <Pickup Floor> <Direction>` (negative for down, positive for up)
4. Simulation step by command: `step [<RepeatNumber>]`

Simulation stops at the end of testing scenario. Use `status` command at the end to get latest elevator state.
