package com.xiaojiaqi.learn.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * @Author: liangjiaqi
 * @Date: 2020/8/27 7:09 PM
 */
public class ProducerInterceptorPrefix implements ProducerInterceptor<String, String> {

    private volatile long succ = 0;
    private volatile long fail = 0;

    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        String s = "prefix-" + record.value();

        return new ProducerRecord<String, String>(record.topic(), record.partition(), record.timestamp(),
                record.key(), s, record.headers());
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        if(exception==null){
            succ++;
        }else {
            fail++;
        }
    }

    @Override
    public void close() {
        System.out.println("发送数据："+succ+":"+fail);
    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
