package libs;

/**
 * Created by nickozoulis on 23/10/2015.
 */
public class Cons {

    /// HBase Configuration constants
    public static final String hbase_IP_address = "127.0.0.1";//127.0.0.1
    public static final String hbase_port = "2181";

    // HBase table constants
    public static final String queries = "queries";
    public static final String batch_views = "batch_views";
    public static final String stream_views = "stream_views";
    public static final String cfQueries = "q"; // column family for Queries table
    public static final String cfViews = "v"; // column family for batch and stream Views
    public static final String qid_ = "qid_";
    public static final String qid_0 = "qid_0";
    public static final String max_qid = "max_qid";
    public static final String clusters = "clusters";
    public static final String filter = "filter";

}
