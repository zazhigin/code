Somefruit Spark Coding Challenge
================================

Implementation of domain events processing.

Domain
------

Say we have an ecommerce site with products divided into categories like toys, electronics etc. We receive events like product was seen (impression), product page was opened, product was purchased etc.

Task #1
-------
Enrich incoming data with user sessions. Definition of a session: for each user, it contains consecutive events that belong to a single category and are not more than 5 minutes away from each other. Output should look like this (session columns are in bold):<br/>
eventTime, eventType, category, userId, ..., ​**sessionId, sessionStartTime, sessionEndTime**<br/>
Implement it using: 1) sql window functions and 2) Spark aggregator.

Task #2
-------
Compute the following statistics:
1. For each category find median session duration
2. For each category find # of unique users spending less than 1 min, 1 to 5 mins and more
than 5 mins
3. For each category find top 10 products ranked by time spent by users on product pages
    - this may require different type of sessions. For this particular task, session lasts until the user is looking at particular product. When particular user switches to another product the new session starts.

Requirements
------------

1. `Scala 2.12.8`
2. `Java 1.8`
3. `SBT 1.2.8`

Building
--------

```
$ git clone https://github.com/zazhigin/code.git
$ cd code/ecommerce
$ sbt compile
```

Cleaning output directory
-------------------------

```
$ rm -r output
```

Execution
---------

#### Starts window duration sessions (Task #1)
```
$ sbt "runMain com.somefruit.domain.sessions.WindowDurationSessions data/events.csv output 5"
$ cat output/windowDurationSessions/part*.csv
```

#### Starts median duration statistics (Task #2)
```
$ sbt "runMain com.somefruit.domain.statistics.MedianDurationStatistics data/duration_sessions.csv output"
$ cat output/medianDurationStatistics/part*.csv
```

#### Starts median duration statistics (Task #2)
```
$ sbt "runMain com.somefruit.domain.statistics.UniqueUsersStatistics data/duration_sessions.csv output 1 5"
$ cat output/uniqueUsersStatistics/lessMin/part*.csv
$ cat output/uniqueUsersStatistics/minMax/part*.csv
$ cat output/uniqueUsersStatistics/grtrMax/part*.csv
```
First CSV file for less than 1 min. Second CSV file for 1 to 5 min. Third CSV file for more than 5 mins.

#### Starts top products ranked by time spent statistics (Task #2)
Starts window product sessions subtask:
```
$ sbt "runMain com.somefruit.domain.sessions.WindowProductSessions data/events.csv output"
$ cat output/windowProductSessions/part*.csv
```
This creates product sessions.

Starts top products ranked by time spent statistics subtask:
```
$ sbt "runMain com.somefruit.domain.statistics.TopProductsStatistics data/duration_sessions.csv output 10"
$ cat output/topProductsStatistics/part*.csv
```
