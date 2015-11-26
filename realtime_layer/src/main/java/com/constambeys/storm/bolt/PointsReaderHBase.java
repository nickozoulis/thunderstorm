package com.constambeys.storm.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import filtering.Point;
import hbase.HReaderPointsC;

import java.io.IOException;
import java.util.Map;

public class PointsReaderHBase implements IRichBolt {

	private int id;
	private String name;
	private OutputCollector collector;
	private HReaderPointsC reader;
	private boolean run;

	public PointsReaderHBase() {
		run = false;
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		this.name = context.getThisComponentId();
		this.id = context.getThisTaskId();

		this.collector = collector;
		try {
			reader = new HReaderPointsC();
			System.out.println("HBASE CONNECTED");
		} catch (IOException e) {
			System.err.println("Points Reader HBase: " + e.getMessage());
		}

		System.out.println("bolt: " + this.name + " id: " + this.id + " prepared #####################");
	}

	@Override
	public void execute(Tuple input) {
		try {
			if (input.getSourceStreamId().equals("commands")) {
				if ("run".equals(input.getStringByField("action"))) {
					run = true;
				}
			}

			if (run) {
				boolean check = false;
				int i = 0;
				Point p;
				if ((p = reader.next()) != null) {
					// Emit the word
					collector.emit(new Values(p));
					System.out.println("Reading new points");
					i++;
					check = true;
				}

				while ((p = reader.next()) != null) {
					// Emit the word
					collector.emit(new Values(p));
					i++;
				}

				if (check)
					System.out.println(i + " points");

			}
			/**
			 * The nextuple it is called forever, so if we have been readed the file we will wait
			 * and then return
			 */
			Thread.sleep(10000);
		} catch (Exception e) {
			System.err.println("PointsReaderHBase: " + e.getMessage());
		}

	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("point"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}