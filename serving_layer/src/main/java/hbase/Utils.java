package hbase;

import com.constambeys.storm.DataFilter;
import com.google.protobuf.ServiceException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by nickozoulis on 20/10/2015.
 */
public class Utils {

    static final Logger logger = LoggerFactory.getLogger(Utils.class);
    private static Configuration config;

    /**
     * Enqueues this KMeans query in an HBase table, where all queries are being stored with unique ID.
     * @param numOfClusters
     */
    public static void queryKMeans(String numOfClusters) {
        try {
            HTable hTable = new HTable(config, Cons.queries);

            // First get max query counter, so as to know how to format the new query key.
            int max_quid = getMaxQueryID(hTable);

            // Increment max query counter.
            max_quid++;

            // Format the put command
            Put p1 = new Put(Bytes.toBytes(Cons.qid_ + max_quid));
            p1.add(Bytes.toBytes(Cons.cfQueries),
                    Bytes.toBytes(Cons.clusters), Bytes.toBytes(numOfClusters));
            hTable.put(p1);
            System.out.println("Inserting query with id: " + max_quid);

            //TODO: Maybe update max counter with a coprocessor.
            updateMaxQueryID(hTable, max_quid);

            hTable.close();
            System.out.println("Table closed");
        } catch (IOException e) {e.printStackTrace();}
    }

    /**
     * Enqueues this constrained KMeans query in an HBase table, where all queries are being stored with unique ID.
     * @param numOfClusters
     */
    public static void queryKMeansConstrained(String numOfClusters, DataFilter filter) {
        try {
            HTable hTable = new HTable(config, Cons.queries);

            // First get max query counter, so as to know how to format the new query key.
            int max_quid = getMaxQueryID(hTable);

            // Increment max query counter.
            max_quid++;

            // Format the put command
            Put p1 = new Put(Bytes.toBytes(Cons.qid_ + max_quid));
            p1.add(Bytes.toBytes(Cons.cfQueries),
                    Bytes.toBytes(Cons.clusters), Bytes.toBytes(numOfClusters));
            p1.add(Bytes.toBytes(Cons.cfQueries),
                    Bytes.toBytes(Cons.filter), Bytes.toBytes(filter.toString()));
            hTable.put(p1);
            System.out.println("Inserting query with id: " + max_quid);
            System.out.println("Filter: " + filter);

            //TODO: Maybe update max counter with a coprocessor.
            updateMaxQueryID(hTable, max_quid);

            hTable.close();
            System.out.println("Table closed");
        } catch (IOException e) {e.printStackTrace();}
    }

    private static int getMaxQueryID(HTable hTable) throws IOException {
        Get g = new Get(Bytes.toBytes(Cons.qid_0));
        Result result = hTable.get(g);
        byte [] value = result.getValue(Bytes.toBytes(Cons.cfQueries), Bytes.toBytes(Cons.max_qid));
        String max_quidString = Bytes.toString(value);
        return Integer.parseInt(max_quidString);
    }

    private static void updateMaxQueryID(HTable hTable, int max_quid) throws IOException {
        Put p2 = new Put(Bytes.toBytes(Cons.qid_0));
        p2.add(Bytes.toBytes(Cons.cfQueries),
                Bytes.toBytes(Cons.max_qid), Bytes.toBytes(Integer.toString(max_quid)));
        hTable.put(p2);
        System.out.println("Max query counter has been updated");
    }

    /**
     * Sets HBaseConfiguration according to constants specified in Cons.java
     */
    public static void setHBaseConfig() {
        config = HBaseConfiguration.create();

        config.set("hbase.zookeeper.quorum", Cons.hbase_IP_address);
        config.set("hbase.zookeeper.property.clientPort", Cons.hbase_port);
    }

    public static Configuration getHBaseConfig() {
        return config;
    }

    /**
     * Tests the connection with hbase and reports.
     * @param config
     */
    public static void testConnectionWithHBase(Configuration config) {
        try {
            System.out.println("HBase connection trial...");
            HBaseAdmin.checkHBaseAvailable(config);
            System.out.println("HBase is running!");
        } catch (MasterNotRunningException e) {
            System.out.println("HBase is not running.");
            e.printStackTrace();
            System.exit(1);
        } catch (IOException e) {e.printStackTrace();
        } catch (ServiceException e) {e.printStackTrace();}
    }

    /**
     * Creates HBase Schema tables, if they do not already exist.
     */
    public static void createSchemaTables() {
        try {
            // Instantiating HbaseAdmin class
            HBaseAdmin admin = new HBaseAdmin(config);

            // Queries table
            // Instantiating table descriptor class
            HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(Cons.queries));
            // Adding column families to table descriptor
            tableDescriptor.addFamily(new HColumnDescriptor(Cons.cfQueries));

            // Create the table through admin
            if (!admin.tableExists(Cons.queries)) {
                admin.createTable(tableDescriptor);
                System.out.println("Table created: " + tableDescriptor.getNameAsString());

                /*
                    Especially for Queries table, add a first specific-case-row with no actual data,
                    just to hold and initialize max query counter to zero.
                */
                HTable hTable = new HTable(config, Cons.queries);
                Put p = new Put(Bytes.toBytes(Cons.qid_0));
                p.add(Bytes.toBytes(Cons.cfQueries),
                        Bytes.toBytes(Cons.max_qid), Bytes.toBytes("0"));
                hTable.put(p);
            } else {
                System.out.println("Table already exists: " + tableDescriptor.getNameAsString());
            }


            // Batch Views table
            tableDescriptor = new HTableDescriptor(TableName.valueOf(Cons.batch_views));
            tableDescriptor.addFamily(new HColumnDescriptor(Cons.cfViews));

            if (!admin.tableExists(Cons.batch_views)) {
                admin.createTable(tableDescriptor);
                System.out.println("Table created: " + tableDescriptor.getNameAsString());
            } else {
                System.out.println("Table already exists: " + tableDescriptor.getNameAsString());
            }


            // Stream Views table
            tableDescriptor = new HTableDescriptor(TableName.valueOf(Cons.stream_views));
            tableDescriptor.addFamily(new HColumnDescriptor(Cons.cfViews));

            if (!admin.tableExists(Cons.stream_views)) {
                admin.createTable(tableDescriptor);
                System.out.println("Table created: " + tableDescriptor.getNameAsString());
            } else {
                System.out.println("Table already exists: " + tableDescriptor.getNameAsString());
            }

            // List all tables.
            HTableDescriptor[] tableDescriptors = admin.listTables();

            // Printing all the table names.
            for (int i=0; i<tableDescriptors.length;i++ ){
                System.out.println(tableDescriptors[i].getNameAsString());
            }

//            admin.shutdown();
        } catch (IOException e) {e.printStackTrace();}
    }

}
