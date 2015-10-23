package com.constambeys.storm.bolt;

import java.util.Map;

import com.constambeys.storm.DataFilter;
import com.constambeys.storm.KMeansOnline;
import com.constambeys.storm.Point;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class BoltProcessor implements IRichBolt {

	Integer id;
	String name;
	KMeansOnline k;
	DataFilter f;
	private OutputCollector collector;

	public BoltProcessor(int k) {
		this.k = new KMeansOnline(k);
		this.f = new DataFilter("{0}+{1}", "<", "{1}+{2}");
	}

	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

		this.collector = collector;
		this.name = context.getThisComponentId();
		this.id = context.getThisTaskId();
		System.out.println("bolt: " + this.name + " id: " + this.id + " prepared #####################");
	}

	public void execute(Tuple input) {
		/**
		 * Handle signal to clear cache
		 */

		if (input.getSourceStreamId().equals("signals")) {
			if ("refresh".equals(input.getStringByField("action"))) {
				// counters.clear();
				return;
			} else if ("print".equals(input.getStringByField("action"))) {
				k.print();
			}
			return;
		}

		Point p = (Point) input.getValue(0);

		k.run(p);
		// Set the tuple as Acknowledge
		collector.ack(input);

	}

	public void cleanup() {

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));

	}

	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
