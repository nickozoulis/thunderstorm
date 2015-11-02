package com.constambeys.storm.spouts;

import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class SignalsSpout extends BaseRichSpout {

	private SpoutOutputCollector collector;

	private void refreshCache() {
		collector.emit("signals", new Values("clear"));
	}

	public void nextTuple() {
		try {
			Thread.sleep(10000);
			collector.emit("signals", new Values("print"));
		} catch (InterruptedException e) {
		}
	}

	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("signals", new Fields("action"));
	}

}