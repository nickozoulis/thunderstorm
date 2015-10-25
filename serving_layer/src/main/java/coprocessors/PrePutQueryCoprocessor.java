package coprocessors;

import hbase.Cons;
import hbase.Utils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;

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
        int max_quid = Utils.getMaxQueryID(hTable);

        // Increment max query counter.
        max_quid++;

        Utils.updateMaxQueryID(hTable, max_quid);

        hTable.close();
        connection.close();
    }

}