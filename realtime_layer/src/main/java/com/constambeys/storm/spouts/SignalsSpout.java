package com.constambeys.storm.spouts;

import java.io.IOException;
import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import hbase.Cons;
import hbase.HMessages;

public class SignalsSpout extends BaseRichSpout {

	private long iter = 0;
	private HMessages messages;
	private SpoutOutputCollector collector;

	public void nextTuple() {
		try {
			Thread.sleep(10000);
			collector.emit("signals", new Values("print"));

			long _iter = messages.read_long(0);
			if (_iter != iter) {
				iter = _iter;
				System.out.println("Resynchonizing kmeans");
				//collector.emit("signals", new Values("clear"));
				collector.emit("signals", new Values("update"));
			}

		} catch (Exception e) {
			System.err.println("Signals Spout: " + e.getMessage());
		}
	}

	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
		try {
			messages = new HMessages(Cons.messages);
			iter = messages.read_long(0);
		} catch (IOException e) {
			System.err.println("Signals Spout: " + e.getMessage());
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("signals", new Fields("action"));
	}

}