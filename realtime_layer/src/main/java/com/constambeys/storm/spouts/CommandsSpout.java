package com.constambeys.storm.spouts;

import java.io.IOException;
import java.util.Map;

import com.constambeys.storm.KMeansOnline;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import filtering.DataFilter;
import hbase.Cons;
import hbase.HReader;
import hbase.KMeans;

public class CommandsSpout extends BaseRichSpout {

	private SpoutOutputCollector collector;
	private HReader reader;

	public void nextTuple() {
		try {

			KMeans km;
			while ((km = reader.next()) != null) {

				KMeansOnline k = new KMeansOnline((int) km.id, km.k);

				for (String filter : km.filters) {
					DataFilter f = new DataFilter(filter);
					k.add(f);
				}

				System.out.println("NEW KMeans " + km.k);
				collector.emit("commands", new Values("kmeans", k));

				collector.emit("commands", new Values("run", null));

				Thread.sleep(5000);
			}
		} catch (Exception e) {
			System.err.println("CommandsSpout: " + e.getMessage());
		}
	}

	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
		try {
			reader = new HReader(Cons.queries);
			System.out.println("HBASE CONNECTED");
		} catch (IOException e) {
			System.err.println("CommandsSpout: " + e.getMessage());
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("commands", new Fields("action", "class"));
	}

}