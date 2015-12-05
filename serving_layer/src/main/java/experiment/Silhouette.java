package experiment;

import filtering.Point;
import java.io.File;

/**
 * @author nickozoulis
 * @author constambeys
 */
public class Silhouette {

    private DataSetReader dataSetReader;
    private ClusterReader clusterReader;
    private int K;

    public Silhouette(String fileName) {
        dataSetReader = new DataSetReader(new File(fileName));
    }

    private double silhouetteCoefficient(Point p) {
        // Distance to all other points
        double distanceA = 0, distanceB = 0;
        int counterA = 0, counterB = 0;

        // Get Point p's cluster
        int c = clusterReader.getCluster(p);

        // Shallow copy in order to save memory
        DataSetReader dr = new DataSetReader(dataSetReader.getDataSet());
        Point pp;

        // Silhouette coefficient calculation:
        // For each point p, first find the average distance between p and all other points in the same cluster
        // (this is a measure of cohesion, call it A)
        // Then find the average distance between p and all points in the nearest cluster
        // (this is a measure of separation from the closest other cluster, call it B)
        while (dr.hasNext()) {
            pp = dr.next();

            int cc, nearestCluster;
            // Foreach point other than p
            if (!pp.equals(p)) {
                // Get its cluster
                cc = clusterReader.getCluster(pp);
                // Get the cluster nearest to cluster c
                nearestCluster = clusterReader.getNearestCluster(c);

                if (cc == c) {
                    // If Point pp is in the same cluster as Point p
                    distanceA += Point.distance(p, pp);
                    counterA++;
                } else if (cc == nearestCluster) {
                    // If Point pp is in the nearest cluster of Point p
                    distanceB += Point.distance(p, pp);
                    counterB++;
                }
            }
        }

        double A = distanceA / counterA;
        double B = distanceB / counterB;

        return (B - A) / Math.max(A, B);
    }

    public static void main(String[] args) {
        Silhouette silhouette = new Silhouette(args[0]);

        File folder = new File(args[1]);

        for (final File file : folder.listFiles()) {
            if (file.getName().endsWith("batch") || file.getName().endsWith("local") || file.getName().endsWith("stream")) {
                silhouette.setClusters(file);
                silhouette.run();
                silhouette.dataSetReader.reset();
            }
        }
    }

    private void setClusters(File file) {
        String[] splits = file.getName().split("_");
        K = Integer.parseInt(splits[1]);

        clusterReader = new ClusterReader(file);
    }

    public void run() {
        double silhouettes = 0;
        int s_counters = 0;
        Point p;

        // Foreach point in the dataSet calculate its silhouette coefficient and find their average
        while (dataSetReader.hasNext()) {
            p = dataSetReader.next();
            silhouettes += silhouetteCoefficient(p);
            s_counters++;
        }

        System.out.println(String.format("%s,%.3f", clusterReader.getFile().getName(), silhouettes / s_counters));
    }

}
