
# same  docker commande
docker stop $(docker ps -aq)
docker rm  $(docker ps -aq)
# Après avoir lancer  le docker compose via lacommande
docker-compose up
# verifier que les containers
docker ps
#######################  Producer ##############################
# pour starter le  kafka producer
docker exec -it broker bash

# Command to produce messages in to the Kafka topic.   ici 'greetings' et le nom du topi
kafka-console-producer --broker-list localhost:9092 --topic greetings
# Publish to produce message topics with key/value  'greetings' topic with key and value
kafka-console-producer --broker-list localhost:9092 --topic orders --property "key.separator=-" --property "parse.key=true"
or
kafka-console-producer --topic first-topic  --bootstrap-server localhost:9092   --property parse.key=true  --property key.separator=":"

exemple de message:
key:my first message
key:is something
key:very simple



# ######################  consumer ##############################


# Command to consumer messages from  Kafka topic.  ici 'greetings_uppercase' et le nom du topic
kafka-console-consumer --bootstrap-server localhost:9092 --topic greetings_uppercase

# Command to consume with Key.  ici 'greetings_uppercase' et le nom du topic
kafka-console-consumer --bootstrap-server localhost:9092 --topic orders --from-beginning -property "key.separator= - " --property "print.key=true"
or
kafka-console-consumer --topic first-topic --bootstrap-server localhost:9092   --from-beginning   --property print.key=true --property key.separator="-"

### or when you specify the partition

kafka-console-consumer --topic second-topic  --bootstrap-server broker:9092  --property print.key=true  --property key.separator="-" --partition 0

# Lister les topics
kafka-topics --bootstrap-server localhost:9092 --list
# delete les topics
kafka-topics --bootstrap-server localhost:9092 --delete --topic  '.*'  // or topicName
# create les topics
kafka-topics --create --topic first-topic --bootstrap-server localhost:9092  --replication-factor 1 --partitions 1
kafka-topics --create --topic second-topic --bootstrap-server localhost:9092  --replication-factor 1 --partitions 2