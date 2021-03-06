package com.constambeys.storm.spouts;

import java.io.IOException;
import java.util.Map;

import clustering.KMeansQuery;
import com.constambeys.storm.KMeansOnline;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import filtering.DataFilter;
import filtering.Point;
import hbase.Cons;
import hbase.HReaderQueriesC;
import hbase.HReaderResultsC;

public class CommandsSpout extends BaseRichSpout {

	private SpoutOutputCollector collector;
	private HReaderQueriesC reader;
	private HReaderResultsC readerBatch;

	public void nextTuple() {
		try {

			KMeansQuery km;
			while ((km = reader.next()) != null) {

				KMeansOnline k = new KMeansOnline((int) km.getId(), km.getK());

				// Add filters
				for (String filter : km.getFilters()) {
					DataFilter f = new DataFilter(filter);
					k.add(f);
				}

				// Initialize state from batch
				Point[] point = readerBatch.get(k.id);
				if (point.length == km.getK()) {
					k.setStart(point);
					System.out.println("NEW KMeans with batch state id=" + km.getId());
				} else {
					if (point.length > 0)
						System.out.println("KMeans Error Cannot initialize from batch" + km.getId());
					System.out.println("NEW KMeans id=" + km.getId());
				}

				collector.emit("commands", new Values("kmeans", k));
			}

			/**
			 * The nextuple it is called forever, so if we have been readed the file we will wait
			 * and then return
			 */
			collector.emit("commands", new Values("run", null));
			Thread.sleep(5000);
		} catch (Exception e) {
			System.err.println("CommandsSpout: " + e.getMessage());
		}
	}

	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
		try {
			reader = new HReaderQueriesC(Cons.queries);
			readerBatch = new HReaderResultsC(Cons.batch_views);
			System.out.println("HBASE CONNECTED");
		} catch (IOException e) {
			System.err.println("CommandsSpout: " + e.getMessage());
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("commands", new Fields("action", "class"));
	}

}