package com.constambeys.storm.bolt;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class PointsReader implements IRichBolt {

	private int id;
	private String name;
	private OutputCollector collector;

	private String filename;
	private boolean completed = false;

	public PointsReader(String filename) {
		this.filename = filename;
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		this.name = context.getThisComponentId();
		this.id = context.getThisTaskId();
		System.out.println("bolt: " + this.name + " id: " + this.id + " prepared #####################");
	}

	@Override
	public void execute(Tuple input) {

		if (input.getSourceStreamId().equals("commands")) {
			if ("kmeans".equals(input.getStringByField("action"))) {

				if (!completed) {
					BufferedReader reader = null;
					try {
						// Open the reader
						reader = new BufferedReader(new FileReader(filename));
						String str;
						// Read all lines
						while ((str = reader.readLine()) != null) {
							/**
							 * By each line emmit a new value with the line as a
							 * their
							 */
							this.collector.emit(new Values(str));
						}
					} catch (Exception e) {
						throw new RuntimeException("Error reading tuple", e);
					} finally {
						completed = true;
						if (reader != null) {
							try {
								reader.close();
							} catch (IOException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
						}
					}
				}
			}
		}

		/**
		 * The nextuple it is called forever, so if we have been readed the file
		 * we will wait and then return
		 */

		try {
			Thread.sleep(10000);
		} catch (InterruptedException e) {
			// Do nothing
		}

	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("line"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}