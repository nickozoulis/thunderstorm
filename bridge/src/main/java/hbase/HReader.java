package hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class HReader {

	private long currentID = 1;
	private HConnection connection;
	private HTableInterface hTable;

	public HReader(String tableName) throws IOException {
		Configuration config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum", Cons.hbase_IP_address);
		config.set("hbase.zookeeper.property.clientPort", Cons.hbase_port);

		connection = HConnectionManager.createConnection(config);
		hTable = connection.getTable(tableName);

	}

	private long getMaxID() throws IOException {

		Get g;
		Result r;

		g = new Get(Bytes.toBytes(Cons.qid_ + 0));
		r = hTable.get(g);

		byte[] value = r.getValue(Bytes.toBytes(Cons.cfQueries), Bytes.toBytes(Cons.max_qid));
		return Bytes.toLong(value);
	}

	public KMeans next() throws IOException {
		long maxID = getMaxID();

		if (currentID <= maxID) {
			Get g = new Get(Bytes.toBytes(Cons.qid_ + currentID));
			Result r = hTable.get(g);

			byte[] valueClusters = r.getValue(Bytes.toBytes(Cons.cfQueries), Bytes.toBytes(Cons.clusters));
			byte[] valueFilter = r.getValue(Bytes.toBytes(Cons.cfQueries), Bytes.toBytes(Cons.filter));

			KMeans km = new KMeans(currentID, Integer.parseInt(Bytes.toString(valueClusters)));

			if (valueFilter != null) {
				String filter = Bytes.toString(valueFilter);
				km.addFilter(filter);
			}

			currentID++;

			return km;
		} else {
			return null;
		}

	}
}
