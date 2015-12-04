package clustering;

import filtering.DataFilter;
import filtering.Point;
import hbase.Cons;
import hbase.HWriterResults;
import hbase.Utils;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

import java.io.BufferedWriter;
import java.io.FileWriter;
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
    private boolean local;
    private long timeStamp;
    private KMeans kMeans;

    public SparkKMeans(JavaRDD<String> dataset, KMeansQuery kmQuery, boolean local, long timeStamp) {
        points = dataset.map(new ParsePoint());
        this.kmQuery = kmQuery;

        // Variables used for experiments
        this.local = local;
        this.timeStamp = timeStamp;

        // Spark KMeans configuration
        kMeans = new KMeans();
        kMeans.setK(kmQuery.getK());
        kMeans.setEpsilon(Cons.epsilon);
        kMeans.setMaxIterations(Cons.iterations);
        kMeans.setRuns(Cons.runs);
        kMeans.setInitializationMode(KMeans.K_MEANS_PARALLEL());
    }

    public void cluster() {
        KMeansModel model = null;
        long startTime = 0, endTime = 0;

        if (kmQuery.getQueryType() == QueryType.KMEANS) {
            startTime = System.currentTimeMillis();
            model = kMeans.run(points.rdd());
            endTime = System.currentTimeMillis();
        } else if (kmQuery.getQueryType() == QueryType.CONSTRAINED_KMEANS) {
            // For the time being only one filter is supported, so one loop will be executed.
            for (String f : kmQuery.getFilters()) {
                startTime = System.currentTimeMillis();
                model = kMeans.run(getFilter(f).rdd());
                endTime = System.currentTimeMillis();
            }
        }

        if (model != null)
            if (!local) {
                logger.info(">> batch [" + kmQuery + "] [duration: " + Math.abs(endTime-startTime) + " ms] <<");
                writeToHBase(model.clusterCenters());
            }
            else {
                logger.info(">> local [" + kmQuery + "] [duration: " + Math.abs(endTime-startTime) + " ms] <<");
                Utils.printResultDataset(model.clusterCenters());
                writeViewToFile(kmQuery.getK(), model.clusterCenters()); // -- SILHOUETTE
            }
    }

    private void writeViewToFile(long k, Vector[] vs) {
        BufferedWriter bw;

        try {
            bw = new BufferedWriter(new FileWriter(Cons.viewsPath + timeStamp + "_" + k + "_local"));

            for (Vector v : vs) {
                double[] point = v.toArray();
                Point p = new Point(point);
                bw.write(p.toString());
                bw.newLine();
            }

            bw.close();
        } catch(IOException e) {e.printStackTrace();}
    }

    private void writeToHBase(Vector[] vectors) {
        try {
            System.out.println("Writing results for query: " + kmQuery);
            HWriterResults hw = new HWriterResults(Cons.batch_views);
            hw.append(kmQuery.getId(), vectors);
            System.out.println("Finished writing results for query: " + kmQuery);
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
