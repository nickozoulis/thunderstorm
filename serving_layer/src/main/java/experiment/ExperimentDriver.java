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
        int i = 0;
        while (true) {
            for (String line : queryList) {
                String[] splits = line.split(" ");
                logger.info("Executing experiment for query " + line);
                ShellUtils.KMeans(ShellUtils.parseKMeans(line, splits));
            }

            if (++i == 10) System.exit(0); // # runs of the query cycles

            try {
                logger.info("Thread sleeping for " + Cons.delay + " seconds.");
                Thread.sleep(Cons.delay);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Cleans up the queries table, making it ready for experiment.
     */
    private static void cleanup() {
        Configuration conf = connection.getConfiguration();
        logger.info("Preparing HBase tables..");

        HBaseUtils.deleteAllSchemaTables();
        HBaseUtils.createSchemaTables();
    }
}
