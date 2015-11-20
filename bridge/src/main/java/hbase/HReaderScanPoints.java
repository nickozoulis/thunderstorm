package hbase;

import clustering.KMeansQuery;
import filtering.Point;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;

public class HReaderScanPoints {

	private long currentID = 0;
	private long maxID=System.currentTimeMillis();
	private HConnection connection;
	private HTableInterface hTable;
	private ResultScanner rs;

	public HReaderScanPoints(String tableName) throws IOException {
		Configuration config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum", Cons.hbase_IP_address);
		config.set("hbase.zookeeper.property.clientPort", Cons.hbase_port);

		connection = HConnectionManager.createConnection(config);
		hTable = connection.getTable(tableName);

		rs = hTable.getScanner(new Scan(Bytes.toBytes(currentID), Bytes.toBytes(5)));
	}

	public void restart() {
		currentID = 1;
	}

	public Point next() throws IOException {
		Result r = rs.next();

		if (r != null) {

			ArrayList<Double> ar = new ArrayList(10);
			int k = 0;
			for (;;) {

				byte[] value = r.getValue(Bytes.toBytes(Cons.cfAttributes), Bytes.toBytes(k));
				if (value == null) break;
				ar.add(Bytes.toDouble(value));
				k++;
			}
			
			Point p = new Point(ar.toArray(new Double[ar.size()]));

			currentID++;

			return p;
		} else {

			if (currentID == maxID) {
				maxID = maxID + 1000;
				rs = hTable.getScanner(new Scan(Bytes.toBytes(currentID), Bytes.toBytes(maxID)));
				return next();
			} else {
				return null;
			}
		}
	}

	public static void main(String argv[]) throws IOException {

		HReaderScanPoints r = new HReaderScanPoints(Cons.raw_data);

		Point p;
		int i=0;
		while ((p=r.next())!=null) {
			System.out.println(p.toString());
			i++;
		}
		System.out.println(i);
	}
}
