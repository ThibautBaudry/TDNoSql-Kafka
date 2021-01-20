# TDNoSQL-Kafka

Kafka solution who provides all the wifi-connection spots in Paris.

Run these following commands:

$ docker exec -it <container_id> bash

$ kafka-topics --create --zookeeper localhost:2181 --topic wifi-nbterminals-count-notifications --replication-factor 1 --partitions 1 --config "cleanup.policy=compact" --config "delete.retention.ms=100" --config "segment.ms=100" --config "min.cleanable.dirty.ratio=0.01" $ kafka-consumer-groups -bootstrap-server localhost:9092 -describe -group 1

$ kafka-topics --zookeeper localhost:2181 --list

$ kafka-topics --delete --zookeeper localhost:2181 --topic wifi-spots-raw $ kafka-topics --delete --zookeeper localhost:2181 --topic wifiSpots-nbTerminals-update $ kafka-topics --delete --zookeeper localhost:2181 --topic wifi-nbterminals-count-notifications
