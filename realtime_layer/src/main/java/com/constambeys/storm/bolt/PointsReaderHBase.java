package com.constambeys.storm.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import hbase.Cons;
import hbase.HReaderScan;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Map;

public class PointsReaderHBase implements IRichBolt {

	private int id;
	private String name;
	private OutputCollector collector;
	private HReaderScan reader;

	public PointsReaderHBase() {
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		this.name = context.getThisComponentId();
		this.id = context.getThisTaskId();

		this.collector = collector;
		try {
			reader = new HReaderScan(Cons.queries);
			System.out.println("HBASE CONNECTED");
		} catch (IOException e) {
			System.err.println("Points Reader HBase: " + e.getMessage());
		}

		System.out.println("bolt: " + this.name + " id: " + this.id + " prepared #####################");
	}

	@Override
	public void execute(Tuple input) {

		if (input.getSourceStreamId().equals("commands")) {
			if ("run".equals(input.getStringByField("action"))) {


			}
		}

		/**
		 * The nextuple it is called forever, so if we have been readed the file we will wait and
		 * then return
		 */

		try

		{
			Thread.sleep(10000);
		} catch (

		InterruptedException e)

		{
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