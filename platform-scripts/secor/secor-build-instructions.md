
### Sandbox

./build-secor.sh sandbox raw_telemetry secor ~/secor-raw
./build-secor.sh sandbox analytics secor ~/secor-me

nohup java -Xms256M -Xmx512M -ea -Dsecor_group=raw -Dlog4j.configuration=log4j.sandbox.properties -Dconfig=secor.sandbox.partition.properties -cp /home/ec2-user/secor-raw/secor-0.2-SNAPSHOT.jar:lib/* com.pinterest.secor.main.ConsumerMain &

nohup java -Xms256M -Xmx512M -ea -Dsecor_group=me -Dlog4j.configuration=log4j.sandbox.properties -Dconfig=secor.sandbox.partition.properties -cp /home/ec2-user/secor-me/secor-0.2-SNAPSHOT.jar:lib/* com.pinterest.secor.main.ConsumerMain &



### Production

mkdir -p ~/secor-me

./build-secor.sh prod raw_telemetry secor ~/secor-raw
./build-secor.sh prod analytics secor ~/secor-me

nohup java -Xms256M -Xmx512M -ea -Dsecor_group=raw -Dlog4j.configuration=log4j.prod.properties -Dconfig=secor.prod.partition.properties -cp /home/ec2-user/secor-raw/secor-0.2-SNAPSHOT.jar:lib/* com.pinterest.secor.main.ConsumerMain &

nohup java -Xms256M -Xmx512M -ea -Dsecor_group=me -Dlog4j.configuration=log4j.prod.properties -Dconfig=secor.prod.partition.properties -cp /home/ec2-user/secor-me/secor-0.2-SNAPSHOT.jar:lib/* com.pinterest.secor.main.ConsumerMain &