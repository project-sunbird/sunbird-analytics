## Instances ##

### Sandbox ###

1. Secor - 54.179.138.122
2. Spark - 52.77.223.20

### Prod ###

1. Secor - 52.77.212.151
2. Spark - 54.169.146.32
3. Cassandra - 54.169.179.102

***

## Services ##

### Sandbox ###

**1) Secor raw telemetry sync process**

Process running on - 54.179.138.122

```sh 
# Check if the process is running
ps -ef | grep secor-raw | grep -v grep

# Command to kill
kill pid

# Command to start the process
nohup java -Xms256M -Xmx512M -ea -Dsecor_group=raw -Dlog4j.configuration=log4j.sandbox.properties -Dconfig=secor.sandbox.partition.properties -cp secor-raw/secor-0.2-SNAPSHOT.jar:lib/* com.pinterest.secor.main.ConsumerMain &
```

**2) Secor derived telemetry sync process**

Process running on - 54.179.138.122

```sh 
# Check if the process is running
ps -ef | grep secor-me | grep -v grep

# Command to kill
kill pid

# Command to start the process
nohup java -Xms256M -Xmx512M -ea -Dsecor_group=me -Dlog4j.configuration=log4j.sandbox.properties -Dconfig=secor.sandbox.partition.properties -cp secor-me/secor-0.2-SNAPSHOT.jar:lib/* com.pinterest.secor.main.ConsumerMain &
```

**3) Cassandra Process**

Process running on - 52.77.223.20

```sh 
# Check if the process is running
ps -ef | grep cassandra | grep -v grep

# Command to kill
kill pid

# Command to start the process
$CASSANDRA_HOME/bin/cassandra &
```

**4) Analytics API**

Process running on - 52.77.223.20

```sh 
# Get the process id
pid=`cat /mnt/data/analytics/api/analytics-api-1.0/RUNNING_PID`

# Check if the process is running
ps pid

# Command to kill
kill pid

# Command to start the process
cd /mnt/data/analytics/api/analytics-api-1.0 
nohup ./start &
```

### Prod ###

**1) Secor raw telemetry sync process**

Process running on - 52.77.212.151

```sh 
# Check if the process is running
ps -ef | grep secor-raw | grep -v grep

# Command to kill
kill pid

# Command to start the process
nohup java -Xms256M -Xmx512M -ea -Dsecor_group=raw -Dlog4j.configuration=log4j.sandbox.properties -Dconfig=secor.sandbox.partition.properties -cp secor-raw/secor-0.2-SNAPSHOT.jar:lib/* com.pinterest.secor.main.ConsumerMain &
```

**2) Secor derived telemetry sync process**

Process running on - 52.77.212.151

```sh 
# Check if the process is running
ps -ef | grep secor-me | grep -v grep

# Command to kill
kill pid

# Command to start the process
nohup java -Xms256M -Xmx512M -ea -Dsecor_group=me -Dlog4j.configuration=log4j.sandbox.properties -Dconfig=secor.sandbox.partition.properties -cp secor-me/secor-0.2-SNAPSHOT.jar:lib/* com.pinterest.secor.main.ConsumerMain &
```

**3) Cassandra Process**

Process running on - 54.169.179.102

```sh 
# Check if the cassandra is running
sudo service cassandra status # Would return "* Cassandra is running"

# Command to kill
sudo service cassandra stop

# Command to start the process
sudo service cassandra start
```

**4) Analytics API**

Process running on - 54.169.146.32

```sh 
# Get the process id
pid=`cat /mnt/data/analytics/api/analytics-api-1.0/RUNNING_PID`

# Check if the process is running
ps pid

# Command to kill
kill pid

# Command to start the process
cd /mnt/data/analytics/api/analytics-api-1.0 
nohup ./start &
```