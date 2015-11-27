package experiment;

import hbase.Cons;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import shell.HBaseUtils;
import shell.ShellUtils;

import java.io.IOException;
import java.util.List;

/**
 * Created by nickozoulis on 26/11/2015.
 */
public class ExperimentDriver {

    private static final Logger logger = Logger.getLogger(ExperimentDriver.class);
    private static HConnection connection;

    public ExperimentDriver(List<String> queryList) {
        HBaseUtils.setHBaseConfig();
        Configuration conf = HBaseUtils.getHBaseConfig();
        try {
            connection = HConnectionManager.createConnection(conf);

            cleanup();

            experiment(queryList);

        } catch (IOException e) {e.printStackTrace();}
    }

    private void experiment(List<String> queryList) {
        for (String line : queryList) {
            String[] splits = line.split(" ");
            ShellUtils.KMeans(ShellUtils.parseKMeans(line, splits));
        }
    }

    /**
     * Cleans up the queries table, making it ready for experiment.
     */
    private static void cleanup() {
        Configuration conf = connection.getConfiguration();
        logger.info("Preparing a new data stream simulation.");

        try {
            HBaseAdmin admin = new HBaseAdmin(conf);
            admin.disableTable(Cons.queries);
            admin.deleteTable(Cons.queries);
            logger.info("Table deleted: " + Cons.queries);

            HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(Cons.queries));
            tableDescriptor.addFamily(new HColumnDescriptor(Cons.cfQueries));

            HTable hTable = new HTable(conf, Cons.queries);
            Put p = new Put(Bytes.toBytes(0l));
            p.add(Bytes.toBytes(Cons.cfQueries),
                    Bytes.toBytes(Cons.max_qid), Bytes.toBytes(0l)); // Zero as Long
            hTable.put(p);

            admin.createTable(tableDescriptor);
            logger.info("Table created: " + tableDescriptor.getNameAsString());
        } catch (IOException e) {
            e.printStackTrace();
        }

        logger.info("Data stream simulation is ready to begin.");
    }
}
