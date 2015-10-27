package libs;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

/** A Test Coprocessor to see how it works
 * Created by nickozoulis on 25/10/2015.
 */

public class PrePutQueryCoprocessor extends BaseRegionObserver {

    static final Logger logger = LoggerFactory.getLogger(PrePutQueryCoprocessor.class);
    private Configuration config;

    @Override
    public void start(CoprocessorEnvironment e) throws IOException {
        logger.info("Coprocessor started.");
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

        hTable.close();
        connection.close();
        logger.info("Coprocessor finished.");
    }

    public static int getMaxQueryID(HTableInterface hTable) throws IOException {
        Get g = new Get(Bytes.toBytes(Cons.qid_0));
        Result result = hTable.get(g);
        byte [] value = result.getValue(Bytes.toBytes(Cons.cfQueries), Bytes.toBytes(Cons.max_qid));
        String max_quidString = Bytes.toString(value);
        return Integer.parseInt(max_quidString);
    }


}