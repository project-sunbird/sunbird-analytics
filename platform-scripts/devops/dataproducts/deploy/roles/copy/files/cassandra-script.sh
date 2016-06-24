

 #check process secor-script with pidfile /var/run/secor-script.pid
 #      start = "/bin/secor-script start"
 #      stop = "/bin/secor-script stop"

#The wrapper script:
 #!/bin/bash
 #CLASSPATH=secor-0.2-SNAPSHOT.jar

 case $1 in
    start)
       echo $$ > /var/run/cassandra-script.pid;
       cd $CASSANDRA_HOME/bin
       exec 2>&1 cassandra & 1>/tmp/cassandra-script.out
       ;;
     
     stop)  
       kill `cat /var/run/cassandra-script.pid` ;;
     
     check)
       if [ -f /var/run/cassandra-script.pid ] then;
          echo "CASSANDRA PROCESS IS RUNNING"
        else
          echo "CASSANDRA PROCESS IS NOT RUNNING"
       fi

      *)  
       echo "usage: secor-script {start|stop}" ;;
 esac
 exit 0