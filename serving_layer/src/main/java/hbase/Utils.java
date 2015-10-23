package hbase;

import com.google.protobuf.ServiceException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.HBaseAdmin;
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

    }

    /**
     * Enqueues this constrained KMeans query in an HBase table, where all queries are being stored with unique ID.
     * @param numOfClusters
     */
    public static void queryKMeansConstrained(String numOfClusters, String constraint) {

    }

    /**
     * Sets HBaseConfiguration
     */
    public static void setHBaseConfig() {
        config = HBaseConfiguration.create();

        config.set("hbase.zookeeper.quorum", Cons.hbase_IP_address);
        config.set("hbase.zookeeper.property.clientPort", Cons.hbase_port);
    }

    public static Configuration getHBaseConfig() {
        return config;
    }

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
            tableDescriptor.addFamily(new HColumnDescriptor(Cons.columnnFamilyQ));

            // Create the table through admin
            if (!admin.tableExists(Cons.queries)) {
                admin.createTable(tableDescriptor);
                System.out.println("Table created: " + tableDescriptor.getNameAsString());
            } else {
                System.out.println("Table already exists: " + tableDescriptor.getNameAsString());
            }


            // Batch Views table
            tableDescriptor = new HTableDescriptor(TableName.valueOf(Cons.batch_views));
            tableDescriptor.addFamily(new HColumnDescriptor(Cons.columnnFamilyV));

            if (!admin.tableExists(Cons.batch_views)) {
                admin.createTable(tableDescriptor);
                System.out.println("Table created: " + tableDescriptor.getNameAsString());
            } else {
                System.out.println("Table already exists: " + tableDescriptor.getNameAsString());
            }


            // Stream Views table
            tableDescriptor = new HTableDescriptor(TableName.valueOf(Cons.stream_views));
            tableDescriptor.addFamily(new HColumnDescriptor(Cons.columnnFamilyV));

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
