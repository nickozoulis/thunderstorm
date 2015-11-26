package hbase;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class HMessages {

	private HConnection connection;
	private HTableInterface hTable;

	public HMessages(String tableName) throws IOException {

		Configuration config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum", Cons.hbase_IP_address);
		config.set("hbase.zookeeper.property.clientPort", Cons.hbase_port);

		connection = HConnectionManager.createConnection(config);
		hTable = connection.getTable(tableName);

	}

	public void write(long id, long value) throws IOException {

		Put put = new Put(Bytes.toBytes(id));

		put.add(Bytes.toBytes(Cons.cfAttributes), Bytes.toBytes(Cons.cfAttributes), Bytes.toBytes(value));

		hTable.put(put);

	}

	public long read_long(long id) throws IOException {

		Get g;
		Result r;

		g = new Get(Bytes.toBytes(0l));
		r = hTable.get(g);

		byte[] value = r.getValue(Bytes.toBytes(Cons.cfAttributes), Bytes.toBytes(Cons.cfAttributes));
		return Bytes.toLong(value);
	}

	public void close() throws IOException {
		hTable.close();
		connection.close();
	}

	public static void main(String argv[]) throws IOException {

		HMessages messages = new HMessages(Cons.messages);
		messages.write(0, 10);
		long iter = messages.read_long(0);
		System.out.println("Finished");
	}

}
