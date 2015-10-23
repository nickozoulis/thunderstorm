package hbase;

/**
 * Created by nickozoulis on 23/10/2015.
 */
public class Cons {

    // HBase Configuration constants
    public static String hbase_IP_address = "192.168.0.9";
    public static String hbase_port = "2181";

    // HBase table constants
    public static String queries = "queries";
    public static String batch_views = "batch_views";
    public static String stream_views = "stream_views";
    public static String columnnFamilyQ = "q"; // column family for Queries table
    public static String columnnFamilyV = "v"; // column familly for batch and stream Views
}
