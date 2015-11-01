package com.constambeys.storm.bolt;

import java.util.Map;

import com.constambeys.storm.KMeansOnline;
import com.constambeys.storm.Point;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class BoltOutput implements IRichBolt {

	int id;
	String name;
	private OutputCollector collector;

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		this.name = context.getThisComponentId();
		this.id = context.getThisTaskId();
		System.out.println("bolt: " + this.name + " id: " + this.id + " prepared #####################");
	}

	@Override
	public void execute(Tuple input) {
		KMeansOnline k = (KMeansOnline) input.getValue(0);
		Point[] result = k.print();
		if (result == null) {
			System.out.print(String.format("KMeans %d, clusters %d Not Initialized\n", k.id, k.k));
		} else {
			System.out.print(String.format("KMeans %d, clusters %d\n", k.id, k.k));

			for (Point p : result) {
				System.out.println(p.print());
			}
			
		}

	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub

	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
