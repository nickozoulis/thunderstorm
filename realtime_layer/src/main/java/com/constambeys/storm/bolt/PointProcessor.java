package com.constambeys.storm.bolt;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import filtering.Point;

public class PointProcessor implements IRichBolt {

	int id;
	String name;
	private OutputCollector collector;

	public PointProcessor() {

	}

	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		this.name = context.getThisComponentId();
		this.id = context.getThisTaskId();
		System.out.println("bolt: " + this.name + " id: " + this.id + " prepared #####################");
	}

	public void execute(Tuple input) {
		String sentence = input.getString(0);
		String[] values = sentence.split(",");

		Point p = new Point(values);

		// Emit the word
		collector.emit(new Values(p));

		// Acknowledge the tuple
		collector.ack(input);

	}

	public void cleanup() {
		// TODO Auto-generated method stub

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("point"));

	}

	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}