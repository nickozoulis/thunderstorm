package hbase;

import java.io.IOException;

import org.apache.spark.mllib.linalg.Vector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import filtering.Point;

public class HWriter {

    private HConnection connection;
    private HTableInterface hTable;

    public HWriter(String tableName) throws IOException {

        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", Cons.hbase_IP_address);
        config.set("hbase.zookeeper.property.clientPort", Cons.hbase_port);

        connection = HConnectionManager.createConnection(config);
        hTable = connection.getTable(tableName);

    }

    public void append(long id, Point[] result) throws IOException {

        Put put = new Put(Bytes.toBytes(id));
        int i = 0;
        for (Point p : result) {
            put.add(Bytes.toBytes(Cons.cfViews), Bytes.toBytes(Cons.clusters_ + i), Bytes.toBytes(p.toString()));
            i++;
        }

        hTable.put(put);

        hTable.close();
        connection.close();
    }

    public void append(long id, Vector[] result) throws IOException {

        Put put = new Put(Bytes.toBytes(id));
        int i = 0;
        for (Vector v : result) {
            put.add(Bytes.toBytes(Cons.cfViews), Bytes.toBytes(Cons.clusters_ + i), Bytes.toBytes(v.toString()));
            i++;
        }

        hTable.put(put);

        hTable.close();
        connection.close();
    }

}
