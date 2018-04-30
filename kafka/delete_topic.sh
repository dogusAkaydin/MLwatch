#Make sure delete.topic.enable=true $KAFKA/config/server.properties
$KAFKA/bin/kafka-topics.sh --zookeeper localhost:2181 --delete --topic $1
