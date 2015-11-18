import clustering.KMeansQuery;
import clustering.QueryType;
import filtering.DataFilter;
import filtering.Point;
import hbase.Cons;
import hbase.HWriter;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;


/**
 * Created by nickozoulis on 18/11/2015.
 * @author nickozoulis
 * @author constambeys
 */
public class SparkKMeans implements Runnable {

    static final Logger logger = LoggerFactory.getLogger(SparkKMeans.class);
    private KMeansQuery kmQuery;
    private JavaRDD<Vector> points;

    public SparkKMeans(KMeansQuery kmQuery) {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("JavaKMeans");
        sparkConf.setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        JavaRDD<String> lines = sc.textFile(Cons.dataset);
        points = lines.map(new ParsePoint());

        this.kmQuery = kmQuery;
    }

    private void cluster() {
        KMeansModel model = null;

        if (kmQuery.getQueryType() == QueryType.KMEANS) {
            model = KMeans.train(points.rdd(), kmQuery.getK(), Cons.iterations, Cons.runs, KMeans.K_MEANS_PARALLEL());
        } else if (kmQuery.getQueryType() == QueryType.CONSTRAINED_KMEANS) {
            // For the time being only one filter is supported, so one loop will be executed.
            for (String f : kmQuery.getFilters()) {
                model = KMeans.train(getFilter(f).rdd(), kmQuery.getK(), Cons.iterations, Cons.runs, KMeans.K_MEANS_PARALLEL());
            }
        }

        if (model != null)
            writeToHBase(model.clusterCenters());
    }

    private void writeToHBase(Vector[] vectors) {
        try {
            System.out.println("Writing results for query: " + kmQuery);
            new HWriter(Cons.batch_views).append(kmQuery.getId(), vectors);
        } catch (IOException e) {e.printStackTrace();}
    }

    private JavaRDD<Vector> getFilter(final String filter) {
        return points.filter(new Function<Vector, Boolean>() {
            DataFilter d = new DataFilter(filter);

            @Override
            public Boolean call(Vector v1) throws Exception {
                return d.run(new Point(v1.toArray()));
            }
        });
    }

    @Override
    public void run() {
        cluster();
    }

    private static class ParsePoint implements Function<String, Vector> {
        @Override
        public Vector call(String line) {
            String[] tok = line.split(",");
            double[] point = new double[tok.length];
            for (int i = 0; i < tok.length; ++i) {
                point[i] = Double.parseDouble(tok[i]);
            }

            return Vectors.dense(point);
        }
    }
}
