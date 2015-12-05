package com.constambeys.storm.bolt;

import java.util.ArrayList;
import java.util.Map;

import com.constambeys.storm.KMeansGroup;
import com.constambeys.storm.KMeansOnline;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import filtering.Point;
import hbase.Cons;
import hbase.HReaderResultsC;

public class BoltProcessor implements IRichBolt {

	Integer id;
	String name;
	ArrayList<KMeansGroup> ks = new ArrayList<>(0);

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
		try {
			/**
			 * Handle signal to clear cache
			 */

			if (input.getSourceStreamId().equals("signals")) {
				if ("clear".equals(input.getStringByField("action"))) {

					// Read batch results
					if (ks.size() != 0) {

						HReaderResultsC readerBatch = new HReaderResultsC(Cons.batch_views);

						for (KMeansGroup kg : ks) {
							KMeansOnline k = kg.getPrint();
							// Print // this.collector.emit(new Values(k));
							k.clear();

							// Initialise state from batch
							Point[] point = readerBatch.get(k.id);
							if (point.length == k.k) {
								k.setStart(point);
							} else {
								if (point.length > 0)
									System.out.println("KMeans Error Cannot initialize from batch" + k.id);
							}
						}

						readerBatch.close();
					}
				} else if ("update".equals(input.getStringByField("action"))) {

					// Update stream results
					if (ks.size() != 0) {
						HReaderResultsC readerBatch = new HReaderResultsC(Cons.batch_views);

						for (KMeansGroup k : ks) {

							// Initialise state from batch
							Point[] point = readerBatch.get(k.getID());

							if (point.length == k.getK()) {
								k.update(point);
							} else {
								if (point.length > 0)
									System.out.println("KMeans Error Cannot initialize from batch" + k.getID());
							}
						}

						readerBatch.close();
					}

				} else if ("print".equals(input.getStringByField("action"))) {
					for (KMeansGroup kg : ks) {
						this.collector.emit(new Values(kg.getPrint()));
					}
				}
				return;
			}

			if (input.getSourceStreamId().equals("commands"))

			{
				if ("kmeans".equals(input.getStringByField("action"))) {
					ks.add(new KMeansGroup((KMeansOnline) input.getValue(1)));
					return;
				}
				return;
			}

			Point p = (Point) input.getValue(0);

			for (KMeansGroup k : ks) {
				k.run(p);
			}
			// Set the tuple as Acknowledge
			collector.ack(input);
		} catch (

		Exception e)

		{
			// Set the tuple as error
			System.err.println("Bolt Processor" + e.getMessage());
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
