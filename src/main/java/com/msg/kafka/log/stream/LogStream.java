package com.msg.kafka.log.stream;

import com.google.common.collect.Maps;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStreamBuilder;

import java.util.Map;

public class LogStream {
    public static void main(String[] args) {
        Map<String, Object> props = Maps.newHashMap();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "tg");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put("auto.offset.reset", "earliest");
        props.put("zookeeper.connect", "localhost:2181");
        props.put("polls", "1");
        StreamsConfig config = new StreamsConfig(props);

        KStreamBuilder builder = new KStreamBuilder();
        builder.stream("test").mapValues(value -> value.toString().length() + "").to("testCount");
//        builder.stream("test").to("testCount");
        KafkaStreams streams = new KafkaStreams(builder, config);
        streams.start();
    }
}
