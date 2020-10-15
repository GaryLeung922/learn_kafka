package com.xiaojiaqi.learn.producer;

import com.xiaojiaqi.learn.interceptor.ProducerInterceptorPrefix;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;

/**
 * @Author: liangjiaqi
 * @Date: 2020/8/26 6:56 PM
 */
public class ProducerFastStart {
    public static final String brokerList = "localhost:9092";
    public static final String topic = "topic-demo";


    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, ProducerInterceptorPrefix.class.getName());
        properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, brokerList);

        // 配置生产者客户端参数并创建KafkaProducer示例
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        // 构建需要发送的消息
        ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, "hello, Kafka!");


        // 发送消息
        try {
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    System.out.println("complete");
                }
            });
        } catch (Exception e) {
            e.printStackTrace();
        }

        // 关闭生产者客户端示例
        producer.close();
    }
}
