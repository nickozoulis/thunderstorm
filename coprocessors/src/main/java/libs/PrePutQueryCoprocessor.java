package libs;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * Created by nickozoulis on 25/10/2015.
 */

public class PrePutQueryCoprocessor extends BaseRegionObserver {

    private Configuration config;

    @Override
    public void start(CoprocessorEnvironment e) throws IOException {
        config = e.getConfiguration();
    }

    @Override
    public void prePut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) throws IOException {
        HConnection connection = HConnectionManager.createConnection(config);
        HTableInterface hTable = connection.getTable(Cons.queries);

        // First get max query counter, so as to know how to format the new query key.
        int max_quid = getMaxQueryID(hTable);

        // Increment max query counter.
        max_quid++;

        updateMaxQueryID(hTable, max_quid);

        hTable.close();
        connection.close();
    }

    public static int getMaxQueryID(HTableInterface hTable) throws IOException {
        Get g = new Get(Bytes.toBytes(Cons.qid_0));
        Result result = hTable.get(g);
        byte [] value = result.getValue(Bytes.toBytes(Cons.cfQueries), Bytes.toBytes(Cons.max_qid));
        String max_quidString = Bytes.toString(value);
        return Integer.parseInt(max_quidString);
    }

    public static void updateMaxQueryID(HTableInterface hTable, int max_quid) throws IOException {
        Put p2 = new Put(Bytes.toBytes(Cons.qid_0));
        p2.add(Bytes.toBytes(Cons.cfQueries),
                Bytes.toBytes(Cons.max_qid), Bytes.toBytes(Integer.toString(max_quid)));
        hTable.put(p2);
    }

}