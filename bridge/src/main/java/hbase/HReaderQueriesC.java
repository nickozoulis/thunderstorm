package hbase;

import clustering.KMeansQuery;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HReaderQueriesC {

	private long currentID = 0;
	private HConnection connection;
	private HTableInterface hTable;
	private ResultScanner rs;
	private boolean reading = false;

	public HReaderQueriesC(String tableName) throws IOException {
		Configuration config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum", Cons.hbase_IP_address);
		config.set("hbase.zookeeper.property.clientPort", Cons.hbase_port);

		connection = HConnectionManager.createConnection(config);
		hTable = connection.getTable(tableName);
	}

	public void restart() {
		currentID = 0;
	}

	private long getMaxID() throws IOException {

		Get g;
		Result r;

		g = new Get(Bytes.toBytes(0l));
		r = hTable.get(g);

		byte[] value = r.getValue(Bytes.toBytes(Cons.cfQueries), Bytes.toBytes(Cons.max_qid));
		return Bytes.toLong(value);
	}

	public KMeansQuery next() throws IOException {
		if (reading) {
			Result r = rs.next();

			if (r != null) {

				byte[] valueClusters = r.getValue(Bytes.toBytes(Cons.cfQueries), Bytes.toBytes(Cons.clusters));
				byte[] valueFilter = r.getValue(Bytes.toBytes(Cons.cfQueries), Bytes.toBytes(Cons.filter));

				KMeansQuery km = new KMeansQuery(currentID + 1, Bytes.toInt(valueClusters));

				if (valueFilter != null) {
					String filter = Bytes.toString(valueFilter);
					km.getFilters().add(filter);
				}

				currentID++;

				return km;
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
}
