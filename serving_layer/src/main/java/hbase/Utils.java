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
    public static void queryKMeans(String numOfClusters) {
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

            //TODO: Maybe update max counter with a coprocessor.
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
    public static void queryKMeansConstrained(String numOfClusters, DataFilter filter) {
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
                    Bytes.toBytes(Cons.filter), Bytes.toBytes(filter.toString()));
            hTable.put(p1);
            System.out.println("Inserting query with id: " + max_quid);
            System.out.println("Filter: " + filter);

            //TODO: Maybe update max counter with a coprocessor.
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
     * Sets HBaseConfiguration according to constants specified in libs.Cons.java
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
            //TODO: Maybe add a column for enabling or disabling this query.

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

    public static void scanTable(String tableName, int option) {
        String table = "";

        if (tableName.equalsIgnoreCase("queries"))
            table = Cons.queries;
        else if (tableName.equalsIgnoreCase("batch"))
            table = Cons.batch_views;
        else if (tableName.equalsIgnoreCase("stream"))
            table = Cons.stream_views;
        else
            return;

        try {
            HConnection connection = HConnectionManager.createConnection(config);
            HTableInterface hTable = connection.getTable(table);

            Scan scan = new Scan();
            ResultScanner scanner = hTable.getScanner(scan);

            if (tableName.equalsIgnoreCase("queries"))
                scanQueriesHTable(scanner, option);
            else if (tableName.equalsIgnoreCase("batch"))
                scanBatchHTable(scanner);
            else if (tableName.equalsIgnoreCase("stream"))
                scanStreamHTable(scanner);

            scanner.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //TODO
    private static void scanStreamHTable(ResultScanner scanner) throws IOException {
        for (Result result = scanner.next(); result != null; result = scanner.next()) {
            String s = "";

            s += " [row:" + Bytes.toString(result.getRow()) + "]";

//            byte[] value = result.getValue(Bytes.toBytes(Cons.cfViews), Bytes.toBytes(Cons.clusters));
//            if (value != null)
//                s += " [clusters:" + Bytes.toString(value) + "]";
//
//            value = result.getValue(Bytes.toBytes(Cons.c), Bytes.toBytes(Cons.filter));
//            if (value != null)
//                s += " [filter:" + Bytes.toString(value) + "]";

            System.out.println(s);
        }
    }

    //TODO
    private static void scanBatchHTable(ResultScanner scanner) throws IOException {
        for (Result result = scanner.next(); result != null; result = scanner.next()) {
            String s = "";

            s += " [row:" + Bytes.toString(result.getRow()) + "]";

//            byte[] value = result.getValue(Bytes.toBytes(Cons.cfViews), Bytes.toBytes(Cons.clusters));
//            if (value != null)
//                s += " [clusters:" + Bytes.toString(value) + "]";
//
//            value = result.getValue(Bytes.toBytes(Cons.c), Bytes.toBytes(Cons.filter));
//            if (value != null)
//                s += " [filter:" + Bytes.toString(value) + "]";

            System.out.println(s);
        }
    }

    /**
     *
     * @param scanner
     * @param option 0 for all columns, 1 for only cluster column, 2 for only filter column
     * @throws IOException
     */
    private static void scanQueriesHTable(ResultScanner scanner, int option) throws IOException {
        for (Result result = scanner.next(); result != null; result = scanner.next()) {
            String s = "";

            s += " [row:" + Bytes.toString(result.getRow()) + "]";

            byte[] value;
            if (option == 0 || option == 1 || option == 2) {
                if (option == 0 || option == 1) {
                    value = result.getValue(Bytes.toBytes(Cons.cfQueries), Bytes.toBytes(Cons.clusters));
                    if (value != null)
                        s += " [clusters:" + Bytes.toString(value) + "]";
                }

                if (option == 0 || option == 2) {
                    value = result.getValue(Bytes.toBytes(Cons.cfQueries), Bytes.toBytes(Cons.filter));
                    if (value != null)
                        s += " [filter:" + Bytes.toString(value) + "]";
                }
            }

            System.out.println(s);
        }
    }

    //TODO
    public static void getRowFromTable(String tableName, String row) {
        String table = "";

        if (tableName.equalsIgnoreCase("queries"))
            table = Cons.queries;
        else if (tableName.equalsIgnoreCase("batch"))
            table = Cons.batch_views;
        else if (tableName.equalsIgnoreCase("stream"))
            table = Cons.stream_views;
        else
            return;

        try {
            HConnection connection = HConnectionManager.createConnection(config);
            HTableInterface hTable = connection.getTable(table);

            if (tableName.equalsIgnoreCase("queries"))
                getRowFromQueriesHTable(row);
            else if (tableName.equalsIgnoreCase("batch"))
                getRowFromBatchHTable(row);
            else if (tableName.equalsIgnoreCase("stream"))
                getRowFromStreamHTable(row);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //TODO
    private static void getRowFromStreamHTable(String row) {

    }

    //TODO
    private static void getRowFromBatchHTable(String row) {

    }

    //TODO
    private static void getRowFromQueriesHTable(String row) {

    }

}
