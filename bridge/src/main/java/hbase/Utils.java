package hbase;

import com.google.protobuf.ServiceException;
import clustering.KMeansQuery;
import clustering.QueryType;
import net.sf.javaml.core.Dataset;
import net.sf.javaml.core.DefaultDataset;
import net.sf.javaml.core.DenseInstance;
import net.sf.javaml.core.Instance;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by nickozoulis on 20/10/2015.
 */
public class Utils {

    static final Logger logger = LoggerFactory.getLogger(Utils.class);
    private static Configuration config;

    public static long getMaxQueryID(HTableInterface hTable) throws IOException {
        Get g = new Get(Bytes.toBytes(0l));
        Result result = hTable.get(g);
        byte[] value = result.getValue(Bytes.toBytes(Cons.cfQueries), Bytes.toBytes(Cons.max_qid));
        return Bytes.toLong(value);
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
                Put p = new Put(Bytes.toBytes(0l));
                p.add(Bytes.toBytes(Cons.cfQueries),
                        Bytes.toBytes(Cons.max_qid), Bytes.toBytes(0l)); // Zero as Long
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


            // Raw Data table
            tableDescriptor = new HTableDescriptor(TableName.valueOf(Cons.raw_data));
            tableDescriptor.addFamily(new HColumnDescriptor(Cons.cfAttributes));

            if (!admin.tableExists(Cons.raw_data)) {
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
     * Scans an HTable and lists its results. Using options will return filtered records.
     * @param hTableName The name of the table to be scanned.
     * @param option 0 return all records, 1 returns plain KMeans, 2 returns filtered KMeans
     */
    private static void listHTable(String hTableName, int option) {
        try {
            HConnection connection = HConnectionManager.createConnection(config);
            HTableInterface hTable = connection.getTable(hTableName);

            if (hTableName.equalsIgnoreCase(Cons.queries))
                listQueriesHTable(hTable, option);
            else if (hTableName.equalsIgnoreCase(Cons.batch_views))
                listBatchHTable(hTable, option);
            else if (hTableName.equalsIgnoreCase(Cons.stream_views))
                listStreamHTable(hTable, option);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Scans an Queries HTable. Using options will return filtered records.
     * @param option 0 return all records, 1 returns plain KMeans, 2 returns filtered KMeans
     */
    public static void listQueriesHTable(int option) {
        listHTable(Cons.queries, option);
    }

    /**
     * Scans an HTable. All records will be returned.
     */
    public static void listQueriesHTable() {
        listHTable(Cons.queries, 0);
    }

    public static void listBatchHTable() {
        listHTable(Cons.batch_views, 0);
    }

    public static void listStreamHTable() {
        listHTable(Cons.stream_views, 0);
    }


    /**
     * Scans the Queries HTable.
     *
     * @param hTable
     * @param option 0 for all records, 1 for only plain KMeans, 2 for Constrained KMeans
     * @throws IOException
     */
    private static void listQueriesHTable(HTableInterface hTable, int option) throws IOException {
        Scan scan = new Scan();
        ResultScanner rs = hTable.getScanner(scan);

        for (Result result : rs) {
            String s = "";
            byte[] valueClusters = result.getValue(Bytes.toBytes(Cons.cfQueries), Bytes.toBytes(Cons.clusters));
            byte[] valueFilter = result.getValue(Bytes.toBytes(Cons.cfQueries), Bytes.toBytes(Cons.filter));

            String row = " [row:" + Bytes.toLong(result.getRow()) + "]";


            if (option == 0) {
                if (valueClusters != null)
                    s += row + " [clusters:" + Bytes.toInt(valueClusters) + "]";

                if (valueFilter != null)
                    s += " [filter:" + Bytes.toString(valueFilter) + "]";
            } else if (option == 1) {

                if (valueClusters != null && valueFilter == null)
                    s += row + " [clusters:" + Bytes.toInt(valueClusters) + "]";
            } else if (option == 2) {
                if (valueFilter != null) {
                    s += row + " [clusters:" + Bytes.toInt(valueClusters) + "]";
                    s += " [filter:" + Bytes.toString(valueFilter) + "]";
                }
            }
            if (!s.isEmpty()) System.out.println(s);
        }

        rs.close();
    }

    //TODO
    private static void listStreamHTable(HTableInterface hTable, int option) {
    }

    //TODO
    private static void listBatchHTable(HTableInterface hTable, int option) {

    }

    public static Result getRowFromBatchViews(long qid) {
        return getRowFromHTable(Cons.batch_views, qid);
    }

    public static Result getRowFromStreamViews(long qid) {
        return getRowFromHTable(Cons.stream_views, qid);
    }

    private static Result getRowFromHTable(String tableName, long qid) {
        // An empty result.
        Result result = new Result();

        try {
            HConnection connection = HConnectionManager.createConnection(config);
            HTableInterface hTable = connection.getTable(tableName);

            Get g = new Get(Bytes.toBytes(qid));
            result = hTable.get(g);

            hTable.close();
            connection.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return result;
    }

    public static void putStreamView(long qid, String view) {
        putView(Cons.stream_views, qid, view);
    }

    public static void putBatchView(long qid, String view) {
        putView(Cons.batch_views, qid, view);
    }

    private static void putView(String tableName, long qid, String view) {
        try {
            HConnection connection = HConnectionManager.createConnection(config);
            HTableInterface hTable = connection.getTable(tableName);

            Put p = new Put(Bytes.toBytes(qid));
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

    public static Result pollStreamViewForResult(long qid) {
        return pollViewForResult(Cons.stream_views, qid);
    }

    private static Result pollViewForResult(String tableName, long qid) {
        Result result;

        while ((result = getRowFromHTable(tableName, qid)).isEmpty()) {
            try {
                Thread.sleep(Cons.delay);
            } catch (InterruptedException e) {
                e.printStackTrace();
                break;
            }
        }

        return result;
    }

    public static Dataset loadClusters(Result result) {
        byte[] valueClusters;
        int k = 0;
        Dataset ds = new DefaultDataset();

        for (;;) {
            valueClusters = result.getValue(Bytes.toBytes(Cons.cfViews), Bytes.toBytes(Cons.clusters_ + k));

            if (valueClusters != null)
                ds.add(createInstance(Bytes.toString(valueClusters)));
            else
                break;

            k++;
        }

        return ds;
    }

    public static Dataset loadClusters(String tableName, long qid) {
        return loadClusters(getRowFromHTable(tableName, qid));
    }

    private static Instance createInstance(String str) {
        String[] splits = str.split(",");

        double[] values = new double[splits.length];

        for (int i=0; i<splits.length; i++)
            values[i] = Double.parseDouble(splits[i]);

        return new DenseInstance(values);
    }


    /**
     * Adapts the results of the HBase to the ML library output.
     * @param result
     * @return The results are returned in an array of Datasets, where each Dataset represents a cluster.
     */
    public static Dataset[] resultToDataset(Result result) {
        byte[] valueClusters;
        int k = 0;
        Dataset ds = new DefaultDataset();

        for (;;) {
            valueClusters = result.getValue(Bytes.toBytes(Cons.cfViews), Bytes.toBytes(Cons.clusters_ + k));

            if (valueClusters != null)
                ds.add(createInstance(Bytes.toString(valueClusters)));
            else
                break;

            k++;
        }

        //FIXME: Write numOfClusters in views so as to avoid this second loop.
        Dataset[] datasets = new Dataset[ds.size()];
        Dataset temp;
        for (int i=0; i<ds.size(); i++) {
            temp = new DefaultDataset();
            temp.add(ds.get(i));
            datasets[i] = temp;
        }

        return datasets;
    }

    /**
     * Enqueues this KMeans query in an HBase table, where all queries are being stored with unique ID.
     * @param query
     * @return The row key of this query.
     */
    public static long putKMeansQuery(KMeansQuery query) {
        long max_quid = -1;
        try {
            HConnection connection = HConnectionManager.createConnection(config);
            HTableInterface hTable = connection.getTable(Cons.queries);

            // Increment the query id counter.
            hTable.incrementColumnValue(Bytes.toBytes(0l), Bytes.toBytes(Cons.cfQueries), Bytes.toBytes(Cons.max_qid), 1);

            // First get max query counter, so as to know how to format the new query key.
            max_quid = getMaxQueryID(hTable);

            // Format the put command
            Put p1 = new Put(Bytes.toBytes(max_quid));
            p1.add(Bytes.toBytes(Cons.cfQueries),
                    Bytes.toBytes(Cons.clusters), Bytes.toBytes(query.getK()));

            if (query.getQueryType() == QueryType.CONSTRAINED_KMEANS) {
                //FIXME: Make it work for more than one filter
                if (query.getFilters().size() == 1) {
                    String filter = "";
                    for (String s : query.getFilters())
                        filter += s;

                    p1.add(Bytes.toBytes(Cons.cfQueries),
                            Bytes.toBytes(Cons.filter), Bytes.toBytes(filter));
                }
            }

            hTable.put(p1);
            System.out.println("Inserting query with id: " + max_quid);
            System.out.println("Query: " + query.toString());

            //TODO: Build a secondary index: each row the numOfClusters, each column the rowkey of the actual queries table

            hTable.close();
            connection.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return max_quid;
    }

    //TODO: Use the secondary index
    public static long getQueryIDIfExists(KMeansQuery query) {
        if (query == null) return -1;

        long rowKey = -1;

        try {
            HConnection connection = HConnectionManager.createConnection(config);

            // First scan queries table to find the query ID.
            HTableInterface hTable = connection.getTable(Cons.queries);
            Scan scan = new Scan();

            ResultScanner rs = hTable.getScanner(scan);

            for (Result result : rs) {
                byte[] valueClusters = result.getValue(Bytes.toBytes(Cons.cfQueries), Bytes.toBytes(Cons.clusters));
                byte[] valueFilter = result.getValue(Bytes.toBytes(Cons.cfQueries), Bytes.toBytes(Cons.filter));

                int clusters;
                KMeansQuery tempQuery;
                if (valueClusters != null) {
                    clusters = Bytes.toInt(valueClusters);

                    String filter;
                    Set set = new HashSet<>();
                    if (valueFilter != null) {
                        filter = Bytes.toString(valueFilter);
                        set.add(filter);
                        tempQuery = new KMeansQuery(clusters, set);
                    } else {
                        tempQuery = new KMeansQuery(clusters);
                    }

                    if (query.equals(tempQuery)) {
                        rowKey = Bytes.toLong(result.getRow());
                        break;
                    }
                }
            }

            hTable.close();
            connection.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return rowKey;
    }

    public static void insertRawData(String[] split) {
        String path = split[1];

        try {
            HConnection connection = HConnectionManager.createConnection(config);
            HTableInterface hTable = connection.getTable(Cons.raw_data);

            BufferedReader br = new BufferedReader(new FileReader(path));
            String line;

            long counter = 0;
            while ((line = br.readLine()) != null) {
                //TODO: Remove
                if (counter == 1000) break;
                String[] splits = line.split("\\s+");

                Put p = new Put(Bytes.toBytes(counter++));

                int numOfAttr = 0;
                double attr;
                boolean flag = true;
                for (int i=11; i<=18; i++) {
                    try {
                        attr = Double.parseDouble(splits[i]);
                        p.add(Bytes.toBytes(Cons.cfAttributes),
                                Bytes.toBytes(numOfAttr++), Bytes.toBytes(attr));
                    } catch(NumberFormatException e) {
                        e.printStackTrace();
                        flag = false;
                    } catch(ArrayIndexOutOfBoundsException e) {
                        e.printStackTrace();
                        flag = false;
                    }
                }

                // A column will contain the number of attributes contained in this row.
                p.add(Bytes.toBytes(Cons.cfAttributes),
                        Bytes.toBytes(Cons.numOfAttr), Bytes.toBytes(numOfAttr));

                if (flag) hTable.put(p);
            }

            hTable.close();
            connection.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
