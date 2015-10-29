package com.constambeys.storm.bolt;

import java.util.ArrayList;
import java.util.Map;

import javax.script.ScriptException;

import com.constambeys.storm.KMeansOnline;
import com.constambeys.storm.Point;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class BoltProcessor implements IRichBolt {

	Integer id;
	String name;
	ArrayList<KMeansOnline> ks = new ArrayList<>(0);

	private OutputCollector collector;

	public BoltProcessor() {

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
			if ("clear".equals(input.getStringByField("action"))) {
				for (KMeansOnline k : ks)
					k.clear();
			} else if ("print".equals(input.getStringByField("action"))) {
				for (KMeansOnline k : ks) {
					this.collector.emit(new Values(k.print()));
				}
			}
			return;
		}

		if (input.getSourceStreamId().equals("commands")) {
			if ("kmeans".equals(input.getStringByField("action"))) {
				ks.add((KMeansOnline) input.getValue(1));
				return;
			}
			return;
		}

		Point p = (Point) input.getValue(0);
		try {
			for (KMeansOnline k : ks) {
				k.run(p);
			}
			// Set the tuple as Acknowledge
			collector.ack(input);
		} catch (ScriptException e) {
			// Set the tuple as error
			collector.fail(input);
		}

	}

	public void cleanup() {

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("output"));
	}

	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
