#!/bin/bash

printf "NOTE: Run 'mvn install' before launching this script otherwise producers will not launch!\n"

nohup mvn exec:java -Dexec.mainClass="com.zubairsaiyed.twitter.TwitterKafkaProducer" &> logs/TwitterKafkaProducer.log &
nohup mvn exec:java -Dexec.mainClass="com.zubairsaiyed.twitter.TwitterTopicKafkaProducer" &> logs/TwitterTopicKafkaProducer.log &

printf "Started Kafka producers in background. Logs located in 'logs' dir. \n\n"
