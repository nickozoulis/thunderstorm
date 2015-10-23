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
    public static String cfQueries = "q"; // column family for Queries table
    public static String cfViews = "v"; // column family for batch and stream Views
    public static String qid_ = "qid_";
    public static String qid_0 = "qid_0";
    public static String max_qid = "max_qid";
    public static String clusters = "clusters";

}
