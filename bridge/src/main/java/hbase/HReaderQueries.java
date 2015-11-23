package hbase;

import java.io.IOException;

import clustering.KMeansQuery;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class HReaderQueries {

	private long currentID = 1;
	private HConnection connection;
	private HTableInterface hTable;

	public HReaderQueries(String tableName) throws IOException {
		Configuration config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum", Cons.hbase_IP_address);
		config.set("hbase.zookeeper.property.clientPort", Cons.hbase_port);

		connection = HConnectionManager.createConnection(config);
		hTable = connection.getTable(tableName);

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
		long maxID = getMaxID();

		if (currentID <= maxID) {
			Get g = new Get(Bytes.toBytes(currentID));
			Result r = hTable.get(g);

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
			return null;
		}
	}
}
