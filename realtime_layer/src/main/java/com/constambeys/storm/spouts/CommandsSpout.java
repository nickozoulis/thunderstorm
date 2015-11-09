package com.constambeys.storm.spouts;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import com.constambeys.storm.Cons;
import com.constambeys.storm.DataFilter;
import com.constambeys.storm.KMeansOnline;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class CommandsSpout extends BaseRichSpout {

	private SpoutOutputCollector collector;
	private HConnection connection;
	private HTableInterface hTable;
	private int currentID = 1;

	public void nextTuple() {
		try {
			Get g;
			Result r;

			g = new Get(Bytes.toBytes(Cons.qid_ + 0));
			r = hTable.get(g);

			byte[] value = r.getValue(Bytes.toBytes(Cons.cfQueries), Bytes.toBytes(Cons.max_qid));
			int maxID = (int)Bytes.toLong(value);

			while (currentID <= maxID) {

				g = new Get(Bytes.toBytes(Cons.qid_ + currentID));
				r = hTable.get(g);

				byte[] valueClusters = r.getValue(Bytes.toBytes(Cons.cfQueries), Bytes.toBytes(Cons.clusters));
				byte[] valueFilter = r.getValue(Bytes.toBytes(Cons.cfQueries), Bytes.toBytes(Cons.filter));

				KMeansOnline k = new KMeansOnline(currentID, Integer.parseInt(Bytes.toString(valueClusters)));

				if (valueFilter != null) {
					String filter = Bytes.toString(valueFilter);
					DataFilter f = new DataFilter(filter);
					k.add(f);
				}

				collector.emit("commands", new Values("kmeans", k));

				currentID++;
			}

			collector.emit("commands", new Values("run", null));

			Thread.sleep(5000);
		} catch (Exception e) {
			System.err.println("CommandsSpout: " + e.getMessage());
		}
	}

	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;

		Configuration config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum", Cons.hbase_IP_address);
		config.set("hbase.zookeeper.property.clientPort", Cons.hbase_port);
		try {
			connection = HConnectionManager.createConnection(config);
			hTable = connection.getTable(Cons.queries);
		} catch (IOException e) {
			System.err.println("CommandsSpout: " + e.getMessage());
		}

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("commands", new Fields("action", "class"));
	}

}