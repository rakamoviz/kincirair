./kafka-topics.sh --bootstrap-server localhost:9092 --alter --topic example.topic --partitions 2

[watermill] 2021/05/04 18:20:11.837461 subscriber.go:345: 	level=ERROR msg="Sarama internal error" consumer_group=test_consumer_group err="kafka: error while consuming example.topic/1: kafka server: The provided member is not known in the current generation." kafka_consumer_uuid=DVRAw5sVtMyUKQVofP566g provider=kafka subscriber_uuid=pEVA5J8mbVzGw2gCfLYwtD topic=example.topic 

https://github.com/Shopify/sarama/issues/1310

[watermill] 2021/05/04 18:19:30.810412 subscriber.go:312: 	level=ERROR msg="Group consume error" consumer_group=test_consumer_group err="read tcp 127.0.0.1:36258->127.0.0.1:9092: i/o timeout" kafka_consumer_uuid=ANYKtVYyuHfpDNHrmPWstH provider=kafka subscriber_uuid=t63JLX7K3L3QyU4cjhd8FR topic=example.topic 

https://github.com/Shopify/sarama/issues/1192


https://github.com/Shopify/sarama/issues/1608