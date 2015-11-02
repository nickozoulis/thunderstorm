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
     *
     * @param numOfClusters
     */
    public static void putQueryKMeans(String numOfClusters) {
        try {
            HConnection connection = HConnectionManager.createConnection(config);
            HTableInterface hTable = connection.getTable(Cons.queries);

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

            updateMaxQueryID(hTable, max_quid);
            System.out.println("Max query counter has been updated");

            hTable.close();
            connection.close();
            System.out.println("Table closed");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Enqueues this constrained KMeans query in an HBase table, where all queries are being stored with unique ID.
     *
     * @param numOfClusters
     */
    public static void putQueryKMeansConstrained(String numOfClusters, String filter) {
        try {
            HConnection connection = HConnectionManager.createConnection(config);
            HTableInterface hTable = connection.getTable(Cons.queries);

            // First get max query counter, so as to know how to format the new query key.
            int max_quid = getMaxQueryID(hTable);

            // Increment max query counter.
            max_quid++;

            // Format the put command
            Put p1 = new Put(Bytes.toBytes(Cons.qid_ + max_quid));
            p1.add(Bytes.toBytes(Cons.cfQueries),
                    Bytes.toBytes(Cons.clusters), Bytes.toBytes(numOfClusters));
            p1.add(Bytes.toBytes(Cons.cfQueries),
                    Bytes.toBytes(Cons.filter), Bytes.toBytes(filter));
            hTable.put(p1);
            System.out.println("Inserting query with id: " + max_quid);
            System.out.println("Filter: " + filter);

            updateMaxQueryID(hTable, max_quid);
            System.out.println("Max query counter has been updated");

            hTable.close();
            connection.close();
            System.out.println("Table closed");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static int getMaxQueryID(HTableInterface hTable) throws IOException {
        Get g = new Get(Bytes.toBytes(Cons.qid_0));
        Result result = hTable.get(g);
        byte[] value = result.getValue(Bytes.toBytes(Cons.cfQueries), Bytes.toBytes(Cons.max_qid));
        String max_quidString = Bytes.toString(value);
        return Integer.parseInt(max_quidString);
    }

    public static void updateMaxQueryID(HTableInterface hTable, int max_quid) throws IOException {
        Put p2 = new Put(Bytes.toBytes(Cons.qid_0));
        p2.add(Bytes.toBytes(Cons.cfQueries),
                Bytes.toBytes(Cons.max_qid), Bytes.toBytes(Integer.toString(max_quid)));
        hTable.put(p2);
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
     *
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
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ServiceException e) {
            e.printStackTrace();
        }
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
            for (int i = 0; i < tableDescriptors.length; i++) {
                System.out.println(tableDescriptors[i].getNameAsString());
            }

//            admin.shutdown();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Scans an HTable. Using options will return filtered records.
     * @param hTableName The name of the table to be scanned.
     * @param option 0 return all records, 1 returns plain KMeans, 2 returns filtered KMeans
     */
    private static void scanHTable(String hTableName, int option) {
        try {
            HConnection connection = HConnectionManager.createConnection(config);
            HTableInterface hTable = connection.getTable(hTableName);

            if (hTableName.equalsIgnoreCase(Cons.queries))
                scanQueriesHTable(hTable, option);
            else if (hTableName.equalsIgnoreCase(Cons.batch_views))
                scanBatchHTable(hTable, option);
            else if (hTableName.equalsIgnoreCase(Cons.stream_views))
                scanStreamHTable(hTable, option);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Scans an Queries HTable. Using options will return filtered records.
     * @param option 0 return all records, 1 returns plain KMeans, 2 returns filtered KMeans
     */
    public static void scanQueriesHTable(int option) {
        scanHTable(Cons.queries, option);
    }

    /**
     * Scans an HTable. All records will be returned.
     */
    public static void scanQueriesHTable() {
        scanHTable(Cons.queries, 0);
    }

    public static void scanBatchHTable() {
        scanHTable(Cons.batch_views, 0);
    }

    public static void scanStreamHTable() {
        scanHTable(Cons.stream_views, 0);
    }


    /**
     * Scans the Queries HTable.
     *
     * @param hTable
     * @param option 0 for all records, 1 for only plain KMeans, 2 for Constrained KMeans
     * @throws IOException
     */
    private static void scanQueriesHTable(HTableInterface hTable, int option) throws IOException {
        Scan scan = new Scan();
        ResultScanner rs = hTable.getScanner(scan);

        for (Result result : rs) {
            String s = "";
            byte[] valueClusters = result.getValue(Bytes.toBytes(Cons.cfQueries), Bytes.toBytes(Cons.clusters));
            byte[] valueFilter = result.getValue(Bytes.toBytes(Cons.cfQueries), Bytes.toBytes(Cons.filter));

            String row = " [row:" + Bytes.toString(result.getRow()) + "]";


            if (option == 0) {
                if (valueClusters != null)
                    s += row + " [clusters:" + Bytes.toString(valueClusters) + "]";

                if (valueFilter != null)
                    s += " [filter:" + Bytes.toString(valueFilter) + "]";
            } else if (option == 1) {

                if (valueClusters != null && valueFilter == null)
                    s += row + " [clusters:" + Bytes.toString(valueClusters) + "]";
            } else if (option == 2) {
                if (valueFilter != null) {
                    s += row + " [clusters:" + Bytes.toString(valueClusters) + "]";
                    s += " [filter:" + Bytes.toString(valueFilter) + "]";
                }
            }
            if (!s.isEmpty()) System.out.println(s);
        }

        rs.close();
    }

    //TODO
    private static void scanStreamHTable(HTableInterface hTable, int option) {
    }

    //TODO
    private static void scanBatchHTable(HTableInterface hTable, int option) {

    }

    public static Result getRowFromHTable(String tableName, String row) {
        // An empty result.
        Result result = new Result();

        try {
            HConnection connection = HConnectionManager.createConnection(config);
            HTableInterface hTable = connection.getTable(tableName);

            Get g = new Get(Bytes.toBytes(Cons.qid_ + row));
            result = hTable.get(g);

            hTable.close();
            connection.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return result;
    }

    public static void putStreamView(int qid, String view) {
        putView(Cons.stream_views, qid, view);
    }

    public static void putBatchView(int qid, String view) {
        putView(Cons.batch_views, qid, view);
    }

    private static void putView(String tableName, int qid, String view) {
        try {
            HConnection connection = HConnectionManager.createConnection(config);
            HTableInterface hTable = connection.getTable(tableName);

            Put p = new Put(Bytes.toBytes(Cons.qid_ + qid));
            p.add(Bytes.toBytes(Cons.cfViews),
                    Bytes.toBytes(Cons.clusters),
                    Bytes.toBytes(view));
            hTable.put(p);
            System.out.println("Inserting view with query id: " + qid);

            hTable.close();
            connection.close();
            System.out.println("Table closed");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
