package com.hanss.acc;

import java.io.IOException;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.TopologyBuilder;
import org.apache.kafka.streams.state.Stores;


public class StatTopology extends Thread{

   @Override
   public void run() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "energy-stat-processor");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        props.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "127.0.0.1:2181");
        props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
        props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        
        TopologyBuilder builder = new TopologyBuilder();
        builder.addSource("SOURCE", new StringDeserializer(), new StringDeserializer(), "EnergyInfo")
               .addProcessor("EProcessor", EProcessor::new, "SOURCE")
               .addStateStore(Stores.create("Counts").withStringKeys().withIntegerValues().inMemory().build(), "EProcessor")
               .addSink("SINK", "EnergyCount", new StringSerializer(), new IntegerSerializer(), "EProcessor");
        
        KafkaStreams stream = new KafkaStreams(builder, props);
        stream.start();
        try{System.in.read();}catch(IOException e){e.printStackTrace();}
        //System.in.read();
        stream.close();
        stream.cleanUp();
    }
}
