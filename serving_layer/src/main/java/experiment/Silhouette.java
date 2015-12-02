package experiment;

import filtering.Point;

import java.io.File;

public class Silhouette {
    private DataSetReader dataSetReader;
    private ClusterReader clusterReader;
    private int kk = 5;
    private File fileDataset, fileClusters;

    public Silhouette(String dataSetFileName, String clusterFileName) {
        fileClusters = new File(clusterFileName);
        fileDataset = new File(dataSetFileName);
        
        String[] splits = fileClusters.getName().split("_");
        kk = Integer.parseInt(splits[1]);

        dataSetReader = new DataSetReader(fileDataset);
        clusterReader = new ClusterReader(fileClusters);
    }

    private double cal(Point p1, int k1) {
        // Distance to all other points
        double distances[] = new double[kk];
        int d_counters[] = new int[kk];

        clusterReader = new ClusterReader(fileClusters);
        Point p2;
        while (clusterReader.hasNext()) {
            p2 = clusterReader.next();

            int k = clusterReader.getCluster(p2);
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
        new Silhouette(args[0], args[1]).run();
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

        System.out.println(siluettes / s_counters);
    }

}
