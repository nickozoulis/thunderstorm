package com.constambeys.storm.bolt;

import java.io.IOException;
import java.util.Map;

import com.constambeys.storm.KMeansOnline;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import filtering.Point;
import hbase.Cons;
import hbase.HWriterResults;

public class BoltOutput implements IRichBolt {

	private int id;
	private String name;
	private OutputCollector collector;
	private HWriterResults writer;

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		this.name = context.getThisComponentId();
		this.id = context.getThisTaskId();

		try {
			writer = new HWriterResults(Cons.stream_views);
		} catch (IOException e) {
			System.err.println("CommandsSpout: " + e.getMessage());
		}

		System.out.println("bolt: " + this.name + " id: " + this.id + " prepared #####################");
	}

	@Override
	public void execute(Tuple input) {
		try {
			KMeansOnline k = (KMeansOnline) input.getValue(0);
			Point[] result = k.result();
			if (result == null) {
				System.out.print(String.format("KMeans %d, clusters %d Not Initialized\n", k.id, k.k));
			} else {
				/*System.out.print(String.format("KMeans %d, clusters %d\n", k.id, k.k));

				for (Point p : result) {
					System.out.println(p.toString());
				}*/

				writer.append(k.id, result);
			}
		} catch (Exception e) {
			System.err.println("BoltOutput: " + e.getMessage());
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
