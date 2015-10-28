package com.constambeys.storm.spouts;

import java.util.Map;

import com.constambeys.storm.DataFilter;
import com.constambeys.storm.KMeansOnline;
import com.constambeys.storm.Point;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class CommandsSpout extends BaseRichSpout {

	private SpoutOutputCollector collector;
	private boolean completed = false;

	public void nextTuple() {

		if (!completed) {
			KMeansOnline k1 = new KMeansOnline(3);
			Point[] means1 = new Point[3];

			for (int i = 0; i < 3; i++) {
				means1[i] = new Point(3);
			}
			k1.init(means1);
			DataFilter f = new DataFilter("{0}", "<", "50");
			k1.add(f);
			collector.emit("commands", new Values("kmeans", k1));

			KMeansOnline k2 = new KMeansOnline(5);
			collector.emit("commands", new Values("kmeans", k2));

			completed = true;
		}

		try {
			Thread.sleep(5000);
		} catch (InterruptedException e) {
		}
	}

	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("commands", new Fields("action", "class"));
	}

}