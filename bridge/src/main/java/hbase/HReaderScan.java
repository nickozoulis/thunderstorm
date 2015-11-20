package hbase;

import clustering.KMeansQuery;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HReaderScan {

	private long currentID = 1;
	private HConnection connection;
	private HTableInterface hTable;
	private ResultScanner rs;

	public HReaderScan(String tableName) throws IOException {
		Configuration config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum", Cons.hbase_IP_address);
		config.set("hbase.zookeeper.property.clientPort", Cons.hbase_port);

		connection = HConnectionManager.createConnection(config);
		hTable = connection.getTable(tableName);

		rs = hTable.getScanner(new Scan(Bytes.toBytes(currentID), Bytes.toBytes(getMaxID())));
	}

	public void restart() {
		currentID = 1;
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
		Result r = rs.next();

		if (r != null) {

			byte[] valueClusters = r.getValue(Bytes.toBytes(Cons.cfQueries), Bytes.toBytes(Cons.clusters));
			byte[] valueFilter = r.getValue(Bytes.toBytes(Cons.cfQueries), Bytes.toBytes(Cons.filter));

			KMeansQuery km = new KMeansQuery(currentID, Bytes.toInt(valueClusters));

			if (valueFilter != null) {
				String filter = Bytes.toString(valueFilter);
				km.getFilters().add(filter);
			}

			currentID++;

			return km;
		} else {
			long maxID = getMaxID();

			if (currentID <= maxID) {
				rs = hTable.getScanner(new Scan(Bytes.toBytes(currentID), Bytes.toBytes(maxID)));
				return next();
			} else {
				return null;
			}
		}
	}
}
