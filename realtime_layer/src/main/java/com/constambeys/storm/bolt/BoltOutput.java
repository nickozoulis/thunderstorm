package com.constambeys.storm.bolt;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import com.constambeys.storm.Cons;
import com.constambeys.storm.KMeansOnline;
import com.constambeys.storm.Point;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class BoltOutput implements IRichBolt {

	private int id;
	private String name;
	private HConnection connection;
	private HTableInterface hTable;
	private OutputCollector collector;

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		this.name = context.getThisComponentId();
		this.id = context.getThisTaskId();

		Configuration config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum", Cons.hbase_IP_address);
		config.set("hbase.zookeeper.property.clientPort", Cons.hbase_port);
		try {
			connection = HConnectionManager.createConnection(config);
			hTable = connection.getTable(Cons.stream_views);
		} catch (IOException e) {
			System.err.println("CommandsSpout: " + e.getMessage());
		}

		System.out.println("bolt: " + this.name + " id: " + this.id + " prepared #####################");
	}

	@Override
	public void execute(Tuple input) {
		try {
			KMeansOnline k = (KMeansOnline) input.getValue(0);
			Point[] result = k.print();
			if (result == null) {
				System.out.print(String.format("KMeans %d, clusters %d Not Initialized\n", k.id, k.k));
			} else {
				// System.out.print(String.format("KMeans %d, clusters %d\n", k.id, k.k));

				// for (Point p : result) {
				// System.out.println(p.print());
				// }

				Put put = new Put(Bytes.toBytes(Cons.qid_ + k.id));
				int i = 0;
				for (Point p : result) {
					put.add(Bytes.toBytes(Cons.cfViews), Bytes.toBytes(Cons.clusters_ + i),
							Bytes.toBytes(p.toString()));
					i++;
				}

				hTable.put(put);

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
