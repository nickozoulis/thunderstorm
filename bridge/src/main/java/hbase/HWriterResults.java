package hbase;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.Arrays;

import org.apache.spark.mllib.linalg.Vector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import filtering.Point;

public class HWriterResults {

	private HConnection connection;
	private HTableInterface hTable;

	public HWriterResults(String tableName) throws IOException {

		Configuration config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum", Cons.hbase_IP_address);
		config.set("hbase.zookeeper.property.clientPort", Cons.hbase_port);

		connection = HConnectionManager.createConnection(config);
		hTable = connection.getTable(tableName);

	}

	public void append(long id, Point[] ps) throws IOException {

		Put put = new Put(Bytes.toBytes(id));
		int i = 0;
		for (Point p : ps) {
			put.add(Bytes.toBytes(Cons.cfViews), Bytes.toBytes(Cons.clusters_ + i), Bytes.toBytes(p.toString()));
			i++;
		}

		hTable.put(put);

	}

	public void append(long id, Vector[] vs) throws IOException {

		Put put = new Put(Bytes.toBytes(id));
		int i = 0;
		for (Vector v : vs) {
			double[] point = v.toArray();
			Point p = new Point(point);
			put.add(Bytes.toBytes(Cons.cfViews), Bytes.toBytes(Cons.clusters_ + i), Bytes.toBytes(p.toString()));
			i++;
		}

		hTable.put(put);
	}

	public void close() throws IOException {
		hTable.close();
		connection.close();
	}

}
