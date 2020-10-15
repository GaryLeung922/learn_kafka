package com.xiaojiaqi.learn.consumer;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * @Author: liangjiaqi
 * @Date: 2020/8/26 7:03 PM
 */
public class ConsumerFastStart {
    public static final String brokerList = "localhost:9092";
    public static final String topic = "topic-demo";
    public static final String groupId = "group.demo";

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        //设置消费组的名称
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        // 创建一个消费者客户端实例
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        //订阅一个主题
        consumer.subscribe(Collections.singletonList(topic));

        // 循环消费消息
        while (true){
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            for(ConsumerRecord<String, String> record: records){
                System.out.println(record.value());
            }
        }
    }
}
