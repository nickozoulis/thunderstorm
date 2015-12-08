
import java.io.File;
import java.util.List;

/**
 * @author nickozoulis
 * @author constambeys
 */
public class Silhouette {

    private DataSetReader dr;
    private ClusterReader cr;
    private int K;

    public Silhouette(DataSetReader dr, ClusterReader cr) {
        this.dr = dr;
        this.cr = cr;
    }

    private double silhouetteCoefficient(Point p) {
        // Distance to all other points
        double distanceA = 0, distanceB = 0;
        int counterA = 0, counterB = 0;

        // Get Point p's cluster
        int c = cr.getCluster(p);
        List<Point> clusterPoints = dr.getClusterDataPoints(c);

        // For each point p, first find the average distance between p and all other points in the same cluster
        // (this is a measure of cohesion, call it A)
        for (Point pp : clusterPoints) {
            // Foreach point other than p
            if (!pp.equals(p)) {
                distanceA += Point.distance(p, pp);
                counterA++;
            }
        }

        // Get Point p's neighbour cluster
        int cc = cr.getNearestCluster(p, c, dr.getSizeOfClusters());
        List<Point> neighbourClusterPoints = dr.getClusterDataPoints(cc);

        // Then find the average distance between p and all points in the nearest cluster
        // (this is a measure of separation from the closest other cluster, call it B)
        for (Point pp : neighbourClusterPoints) {
            distanceB += Point.distance(p, pp);
            counterB++;
        }

        // Take care of division by zero (zero or one point assigned to a cluster)
        double A = 0;
        if  (counterA > 0)
            A = distanceA / counterA;
        double B = distanceB / counterB;

        double nan = (B - A) / Math.max(A, B);
        if (Double.isNaN(nan))
            System.out.println();
        return (B - A) / Math.max(A, B);
    }

    private void setNumOfClusters(File file) {
        String[] splits = file.getName().split("_");
        this.K = Integer.parseInt(splits[1]);
    }

    public void run() {
        double silhouettes = 0;
        int s_counters = 0;
        Point p;

        // Foreach point in the dataSet calculate its silhouette coefficient and find their average
        while (dr.hasNext()) {
            p = dr.next();
            silhouettes += silhouetteCoefficient(p);
            s_counters++;
        }

        System.out.println(String.format("%s,%.3f", cr.getFile().getName(), silhouettes / s_counters));
    }

    public static void main(String[] args) {
        File dFile = new File(args[0]);
        File folder = new File(args[1]);

        ClusterReader cr;
        DataSetReader dr;
        Silhouette silhouette;

        for (final File cFile : folder.listFiles()) {
            if (cFile.getName().endsWith("batch") || cFile.getName().endsWith("local") || cFile.getName().endsWith("stream")) {
                cr = new ClusterReader(cFile);
                dr = new DataSetReader(dFile, cr);
                silhouette = new Silhouette(dr, cr);
                silhouette.setNumOfClusters(cFile);
                silhouette.run();
            }
        }
    }

}
