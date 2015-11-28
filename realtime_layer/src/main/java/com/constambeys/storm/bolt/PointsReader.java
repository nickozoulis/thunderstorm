package com.constambeys.storm.bolt;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
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
			if ("run".equals(input.getStringByField("action"))) {

				if (!completed) {
					BufferedReader reader = null;
					try {
						// Open the reader
						System.out.println("Opening file " + filename);

						InputStream is = getClass().getResourceAsStream(filename);

						reader = new BufferedReader(new InputStreamReader(is));

						String str;
						while ((str = reader.readLine()) != null) {

							// By each line emmit a new value with the line as a their
							this.collector.emit(new Values(str));
						}

						System.out.println("Closing file " + filename);
					} catch (Exception e) {
						System.err.println("PointsReader: ");
						e.printStackTrace();
						throw new RuntimeException("Error reading tuple", e);
					} finally {
						completed = true;
						if (reader != null) {
							try {
								reader.close();
							} catch (IOException e) {
							}
						}
					}
				}
			}
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