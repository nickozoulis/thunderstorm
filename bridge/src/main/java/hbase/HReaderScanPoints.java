package hbase;

import filtering.Point;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;

public class HReaderScanPoints {

	private long currentID = 0;
	private HConnection connection;
	private HTableInterface hTable;
	private ResultScanner rs;

	public HReaderScanPoints() throws IOException {
		Configuration config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum", Cons.hbase_IP_address);
		config.set("hbase.zookeeper.property.clientPort", Cons.hbase_port);

		connection = HConnectionManager.createConnection(config);
		hTable = connection.getTable(Cons.raw_data);
		Scan scan = new Scan(Bytes.toBytes(currentID));
		scan.setCaching(100);
		rs = hTable.getScanner(scan);
	}

	public void restart() {
		currentID = 0;
	}

	public Point next() throws IOException {
		Result r = rs.next();

		if (r != null) {

			ArrayList<Double> ar = new ArrayList(10);
			int k = 0;
			for (;;) {

				byte[] value = r.getValue(Bytes.toBytes(Cons.cfAttributes), Bytes.toBytes(k));
				if (value == null)
					break;
				ar.add(Bytes.toDouble(value));
				k++;
			}

			Point p = new Point(ar.toArray(new Double[ar.size()]));

			currentID = Bytes.toLong(r.getRow());

			return p;
		} else {
			Scan scan = new Scan(Bytes.toBytes(currentID + 1));
			scan.setCaching(100);
			rs = hTable.getScanner(scan);
			return null;
		}
	}

	public static void main(String argv[]) throws IOException {

		HReaderScanPoints r = new HReaderScanPoints();

		Point p;
		int i = 0;
		while ((p = r.next()) != null) {
			System.out.println(p.toString());
			i++;
		}
		
		System.out.println(i);
	}
}
