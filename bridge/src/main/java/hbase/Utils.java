package hbase;

import filtering.Point;
import net.sf.javaml.core.DenseInstance;
import net.sf.javaml.core.Instance;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Vector;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by nickozoulis on 20/10/2015.
 */
public class Utils {

    private static final Logger logger = Logger.getLogger(Utils.class);
    private static JavaSparkContext sc;

    public static HConnection initHBaseConnection() {
        try {
            Configuration config = HBaseConfiguration.create();
            config.set("hbase.zookeeper.quorum", Cons.hbase_IP_address);
            config.set("hbase.zookeeper.property.clientPort", Cons.hbase_port);

            return HConnectionManager.createConnection(config);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static JavaRDD<String> loadClusters(Result r) {
        byte[] value = r.getValue(Bytes.toBytes(Cons.cfAttributes), Bytes.toBytes(Cons.numOfAttr));
        int numOfAttr = 8;

        List<String> dataSet = new ArrayList<>(Cons.K);

        byte[] valueClusters;
        int k = 0;
        for (;;) {
            valueClusters = r.getValue(Bytes.toBytes(Cons.cfViews), Bytes.toBytes(Cons.clusters_ + k));

            if (valueClusters != null)
                 dataSet.add(Bytes.toString(valueClusters));
            else
                break;

            k++;
        }

        return sc.parallelize(dataSet, numOfAttr);
    }

    public static void initSparkContext() {
        // Set Spark configuration
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("SparKMeans");
        sparkConf.setMaster("local");
        sc = new JavaSparkContext(sparkConf);
    }

    public static void printResultDataset(Vector[] clusters) {
        for (Vector v : clusters) {
            double[] point = v.toArray();
            System.out.println(new Point(point));
        }
    }

}
