package com.msg.kafka.log.source;

import com.alibaba.fastjson.JSON;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.TimeoutException;

import java.util.Properties;

public class LogProducer {
    public static void main(String[] args) throws InterruptedException {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "0");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("auto.offset.reset", "earliest");
        Producer<String, String> producer = new KafkaProducer<String, String>(props);
        for (int i = 0; i < 100000; i++) {
            System.out.println(i);
            final ProducerRecord<String, String> record = new ProducerRecord<String, String>("test", Integer.toString(i), Integer.toString(i));

            Callback callback = new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (null == e) {
                        System.out.println(JSON.toJSONString(recordMetadata));
                        return;
                    }
                    if (e instanceof InterruptException) {
                        System.out.println("如果线程在阻塞中断。");
                    } else if (e instanceof SerializationException) {
                        System.out.println("如果key或value不是给定有效配置的serializers。");
                    } else if (e instanceof TimeoutException) {
                        System.out.println("如果获取元数据或消息分配内存话费的时间超过max.block.ms。");
                    } else if (e instanceof KafkaException) {
                        System.out.println("Kafka有关的错误（不属于公共API的异常）");
                    }
                }
            };
            producer.send(record, callback);
            Thread.sleep(1000);
        }
//
        producer.close();
        System.out.println("---done");
    }
}
