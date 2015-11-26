import clustering.KMeansQuery;
import clustering.QueryType;
import filtering.DataFilter;
import filtering.Point;
import hbase.Cons;
import hbase.HWriterResults;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

import java.io.IOException;


/**
 * Created by nickozoulis on 18/11/2015.
 * @author nickozoulis
 * @author constambeys
 */
public class SparkKMeans implements Runnable {

    private static final Logger logger = Logger.getLogger(SparkKMeans.class);
    private KMeansQuery kmQuery;
    private JavaRDD<Vector> points;

    public SparkKMeans(JavaRDD<String> dataset, KMeansQuery kmQuery) {
        points = dataset.map(new ParsePoint());
        this.kmQuery = kmQuery;
    }

    private void cluster() {
        KMeansModel model = null;
        long startTime = 0, endTime = 0;

        if (kmQuery.getQueryType() == QueryType.KMEANS) {
            startTime = System.currentTimeMillis();
            model = KMeans.train(points.rdd(), kmQuery.getK(), Cons.iterations, Cons.runs, KMeans.K_MEANS_PARALLEL());
            endTime = System.currentTimeMillis();
        } else if (kmQuery.getQueryType() == QueryType.CONSTRAINED_KMEANS) {
            // For the time being only one filter is supported, so one loop will be executed.
            for (String f : kmQuery.getFilters()) {
                startTime = System.currentTimeMillis();
                model = KMeans.train(getFilter(f).rdd(), kmQuery.getK(), Cons.iterations, Cons.runs, KMeans.K_MEANS_PARALLEL());
                endTime = System.currentTimeMillis();
            }
        }

        logger.info(">> batch [ qid: " + kmQuery.getId() + ", duration: " + Math.abs(endTime-startTime) + " ms ] <<");

        if (model != null)
            writeToHBase(model.clusterCenters());
    }

    private void writeToHBase(Vector[] vectors) {
        try {
            System.out.println("Writing results for query: " + kmQuery);
            HWriterResults hw = new HWriterResults(Cons.batch_views);
            hw.append(kmQuery.getId(), vectors);
            hw.close();
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
