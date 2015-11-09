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

import java.io.IOException;

/** A coprocessor for incrementing max query id in HTable Queries.
 * Created by nickozoulis on 09/11/2015.
 */

public class IncrementMaxQIDCoprocessor extends BaseRegionObserver {

    static final Logger logger = LoggerFactory.getLogger(IncrementMaxQIDCoprocessor.class);
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

        Increment incr = new Increment(Bytes.toBytes(Cons.qid_0));
        incr.addColumn(Bytes.toBytes(Cons.cfQueries), Bytes.toBytes(Cons.max_qid), 1);
        hTable.increment(incr);

        hTable.close();
        connection.close();
        logger.info("Coprocessor finished.");
    }

}