package hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import filtering.Point;

import java.io.IOException;
import java.util.LinkedList;

public class HReaderResultsC {

	private HConnection connection;
	private HTableInterface hTable;

	public HReaderResultsC(String tableName) throws IOException {
		Configuration config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum", Cons.hbase_IP_address);
		config.set("hbase.zookeeper.property.clientPort", Cons.hbase_port);

		connection = HConnectionManager.createConnection(config);
		hTable = connection.getTable(tableName);

	}

	public Point[] get(long id) throws IOException {

		Get g = new Get(Bytes.toBytes(id));
		Result result = hTable.get(g);

		int k = 0;
		LinkedList<Point> points = new LinkedList<Point>();

		while (true) {
			byte[] valueClusters = result.getValue(Bytes.toBytes(Cons.cfViews), Bytes.toBytes(Cons.clusters_ + k));

			if (valueClusters != null) {
				String point = Bytes.toString(valueClusters);
				Point p = new Point(point.split(","));
				points.add(p);
			} else
				break;

			k++;
		}
		
		return points.toArray(new Point[points.size()]);
	}

	public void close() throws IOException {
		hTable.close();
		connection.close();
	}

	public static void main(String argv[]) throws IOException {

		HReaderResultsC r = new HReaderResultsC(Cons.batch_views);

		Point[] points = r.get(1);
		Point[] points2 = r.get(154849);
		
		System.out.println("Finished");
	}
}
