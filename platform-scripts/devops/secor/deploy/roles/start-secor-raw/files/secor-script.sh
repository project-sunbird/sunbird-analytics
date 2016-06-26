

# check process secor-script with pidfile /var/run/secor-script.pid
# start = "/bin/secor-script start"
# stop = "/bin/secor-script stop"

#The wrapper script:
#!/bin/bash

case $1 in
    start)
       echo $$ > /var/run/secor-script.pid
       cd /home/ec2-user/secor-raw
       exec 2>&1 nohup java -Xms256M -Xmx512M -ea -Dsecor_group=raw -Dlog4j.configuration=log4j.{{ env }}.properties -Dconfig=secor.{{ env }}.partition.properties - cp secor-{{ secor.version }}-SNAPSHOT.jar:lib/* com.pinterest.secor.main.ConsumerMain & 1>nohup.out 
       ;;
     
     stop)  
       kill `cat /var/run/secor-script.pid` ;;
     *)  
       echo "usage: secor-script {start|stop|check}" ;;
esac
exit 0