# cassandra-count
Count rows in Cassandra Table

Simple program to count the number of records in a Cassandra table.
By splitting the token range using the numSplits parameter, you can
reduce the amount each query is counting and reduce the probability
of timeouts.

It is true the Spark is well-suited to this operation, however the
goal of this program is to be a simple utility that does not require
Spark.  This is useful for simple debugging of loading data (and other
data quality tasks).  Spark provides much (MUCH) more than this utility
is addressing - I highly recommend Spark+Cassandra.

## Getting it

### Downloading
This utility has already been built, and is available at
https://github.com/brianmhess/cassandra-count/releases/download/v0.0.6/cassandra-count

Get it with wget:
```
wget https://github.com/brianmhess/cassandra-count/releases/download/v0.0.6/cassandra-count
```

You can change the permissions on that file to executable and execute it
directly.  It is also a proper jar file so you can also run via
```
java -jar cassandra-count
```
And add whatever extra arguments you want.  For example:
```
java -jar cassandra-count -Xmx1G
```

### Building
To build this repository, simply clone this repo and run:
```
gradle buildit
```

All of the dependencies are included (namely, the Java driver - currently
version 3.0.0).  The output will be the cassandra-loader executable
in the build directory.  There will also be an jar with all of the
dependencies included in the build/libs/cassandra-count-uber-<version>.jar

## Documentation 
To extract this README document, simply run (on the cassandra-count
executable - (e.g., on build/cassandra-count):
```
jar xf cassandra-count README.md
```

##Usage
```
version: 0.0.6
Usage: -host <ipaddress> -keyspace <ks> -table <tableName> [OPTIONS]
OPTIONS:
  -configFile <filename>         File with configuration options [none]
  -port <portNumber>             CQL Port Number [9042]
  -user <username>               Cassandra username [none]
  -pw <password>                 Password for user [none]
  -ssl-truststore-path <path>    Path to SSL truststore [none]
  -ssl-truststore-pw <pwd>       Password for SSL truststore [none]
  -ssl-keystore-path <path>      Path to SSL keystore [none]
  -ssl-keystore-pw <pwd>         Password for SSL keystore [none]
  -consistencyLevel <CL>         Consistency level [LOCAL_ONE]
  -beginToken <tokenString>      Begin token [none]
  -endToken <tokenString>        End token [none]
  -numFutures <numfutures>       Number of futures [100]
  -numSplits <numsplits>         Number of total splits (0 for <number of tokens>, -1 for size-related generated splits) [number of tokens]
  -splitSize <splitSize>         Split size in MBs [2]
  -debug <0|1|2>                 Print debug messages [0]
```

##Options:
 Switch           | Option             | Default                    | Description
-----------------:|-------------------:|---------------------------:|:----------
 `-host`          | IP Address         | <REQUIRED>                 | Cassandra connection point - required.
 `-keyspace`      | Keyspace Name      | <REQUIRED>                 | Cassandra keyspace - required.
 `-table`         | Table Name         | <REQUIRED>                 | Cassandra table name - required.
 `-configFile`    | Filename           | none                       | Filename of configuration options 
 `-port`          | Port Number        | 9042                       | Cassandra native protocol port number
 `-user`          | Username           | none                       | Cassandra username
 `-pw`            | Password           | none                       | Cassandra password
 `-ssl-truststore-path` | Truststore Path     | none                | Path to SSL truststore
 `-ssl-truststore-pwd`  | Truststore Password | none                | Password to SSL truststore
 `-ssl-keystore-path`   | Keystore Path       | none                | Path to SSL keystore
 `-ssl-keystore-path`   | Keystore Password   | none                | Password to SSL keystore
 '-consistencyLevel | Consistency Level | LOCAL_ONE                 | CQL Consistency Level
 `-numSplits`    | Number of Splits  | Number of Token Ranges       | Number of splits/queries to create 
 `-numFutures`    | Number of Futures  | 1000                       | Number of Java driver futures in flight.
 `-splitSize`     | Size of Split in MB  | 16                       | Split size in MB
 `-debug`    | Debug mode  | 0                       | Debug printing verbosity (0=none, 1=some, 2=verbose)

##Examples
```./cassandra-count -host 127.0.0.1 -keyspace test -table itest```
