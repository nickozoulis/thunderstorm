package experiment;

import filtering.Point;
import java.io.File;

public class Silhouette {

    private DataSetReader dataSetReader;
    private ClusterReader clusterReader;
    private int kk = 5;

    public Silhouette(String fileName) {
        dataSetReader = new DataSetReader(new File(fileName));
    }

    private double cal(Point p1, int k1) {
        // Distance to all other points
        double distances[] = new double[kk];
        int d_counters[] = new int[kk];

        ClusterReader cr = new ClusterReader(clusterReader.getFile());
        Point p2;
        while (cr.hasNext()) {
            p2 = cr.next();

            int k = cr.getCluster(p2);
            distances[k] = distances[k] + Point.distance(p1, p2);
            d_counters[k]++;
        }

        // Average distance to all other points in its cluster
        double a = distances[k1] / d_counters[k1];

        // Find minimum
        double b = Double.MAX_VALUE;
        for (int i = 0; i < kk; i++) {
            if (i == k1)
                continue;

            if (b > (distances[i] / d_counters[i])) {
                b = (distances[i] / d_counters[i]);
            }
        }

        double s = (b - a) / Math.max(a, b);
        return s;
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
        kk = Integer.parseInt(splits[1]);

        clusterReader = new ClusterReader(file);
    }

    public void run() {
        double siluettes = 0;
        int s_counters = 0;

        Point p1;

        double s;
        while (dataSetReader.hasNext()) {
            p1 = dataSetReader.next();
            s = cal(p1, clusterReader.getCluster(p1));
            siluettes = siluettes + s;
            s_counters++;

        }

        System.out.println(String.format("%s : %.3f", clusterReader.getFile().getName(), siluettes / s_counters));
    }

}
