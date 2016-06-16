
package com.mycompany.testkafkaspout;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

public class PrintBolt extends BaseBasicBolt {

    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector boc) {
        System.out.println("Received message, content: " + tuple.getStringByField("message"));
    }

}
