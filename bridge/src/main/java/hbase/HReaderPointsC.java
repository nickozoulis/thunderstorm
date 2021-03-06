package hbase;

import filtering.Point;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;

public class HReaderPointsC {

	private long currentID = 0;
	private HConnection connection;
	private HTableInterface hTable;
	private ResultScanner rs;
	private boolean reading = false;

	public HReaderPointsC() throws IOException {
		Configuration config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum", Cons.hbase_IP_address);
		config.set("hbase.zookeeper.property.clientPort", Cons.hbase_port);

		connection = HConnectionManager.createConnection(config);
		hTable = connection.getTable(Cons.raw_data);
	}

	public void restart() {
		currentID = 0;
	}

	public Point next() throws IOException {
		if (reading) {

			Result r = rs.next();

			if (r != null) {
				reading = true;
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
				rs.close();
				reading = false;
				return null;
			}
		} else {

			Scan scan = new Scan(Bytes.toBytes(currentID + 1));
			scan.setCaching(100);
			rs = hTable.getScanner(scan);
			reading = true;
			return next();
		}
	}

	public static void main(String argv[]) throws IOException {

		HReaderPointsC r = new HReaderPointsC();

		Point p;
		int i = 0;
		while ((p = r.next()) != null) {
			System.out.println(p.toString());
			i++;
		}

		System.out.println(i);
	}
}
