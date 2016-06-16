package com.mycompany.testkafkaspout;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.kafka.spout.KafkaSpoutStreams;
import org.apache.storm.kafka.spout.KafkaSpoutTupleBuilder;
import org.apache.storm.kafka.spout.KafkaSpoutTuplesBuilder;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class Main {

    public static void main(String[] args) throws Exception {
        Config stormConf = new Config();

        String topic = "testTopic";
        
        Map<String, Object> props = new HashMap<>();
        props.put(KafkaSpoutConfig.Consumer.BOOTSTRAP_SERVERS, "127.0.0.1:9092");
        props.put(KafkaSpoutConfig.Consumer.GROUP_ID, "kafkaSpoutTestGroup");
        props.put(KafkaSpoutConfig.Consumer.KEY_DESERIALIZER, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(KafkaSpoutConfig.Consumer.VALUE_DESERIALIZER, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(KafkaSpoutConfig.Consumer.ENABLE_AUTO_COMMIT, "false");
        
        KafkaSpoutStreams kafkaSpoutStreams = new KafkaSpoutStreams.Builder(new Fields("message"), new String[]{topic}).build();
        
        KafkaSpoutTuplesBuilder tuplesBuilder = new KafkaSpoutTuplesBuilder.Builder<>(new TuplesBuilder(topic)).build();
        
        KafkaSpoutConfig spoutConf = new KafkaSpoutConfig.Builder<String, String>(props, kafkaSpoutStreams, tuplesBuilder).build();
        
        KafkaSpout<String, String> spout = new KafkaSpout<>(spoutConf);
        
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("kafka-spout", spout)
            .setNumTasks(1);
        topologyBuilder.setBolt("printer-bolt", new PrintBolt())
            .shuffleGrouping("kafka-spout")
            .setNumTasks(1);

        stormConf.setNumWorkers(1);
        stormConf.setMaxSpoutPending(1000);
        StormSubmitter.submitTopology("kafkaTestTopology", stormConf, topologyBuilder.createTopology());
    }

    private static class TuplesBuilder extends KafkaSpoutTupleBuilder<String, String> {

        public TuplesBuilder(String topic){
            super(topic);
        }
        
        @Override
        public List<Object> buildTuple(ConsumerRecord<String, String> consumerRecord) {
            return new Values(consumerRecord.value());
        }

    }

}
