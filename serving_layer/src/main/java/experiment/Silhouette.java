package experiment;

import filtering.Point;

public class Silhouette {
    FileClusterReader r1;
    FileClusterReader r2;
    int kk = 5;

    public Silhouette(String fileName) {
        String[] splits = fileName.split("_");
        kk = Integer.parseInt(splits[1]);

        r1 = new FileClusterReader(fileName);
        r2 = new FileClusterReader(fileName);
    }

    private double cal(Point p1, int k1) {
        // Distance to all other points
        double distances[] = new double[kk];
        int d_counters[] = new int[kk];

        r2 = new FileClusterReader("");
        Point p2;
        while (r2.hasNext()) {
            p2 = r2.next();

            int k = r2.getCluster(p2);
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
        new Silhouette(args[0]).run();
    }

    public void run() {
        double siluettes = 0;
        int s_counters = 0;

        Point p1;

        double s;
        while (r1.hasNext()) {
            p1 = r1.next();
            s = cal(p1, r1.getCluster(p1));
            siluettes = siluettes + s;
            s_counters++;

        }

        System.out.println(siluettes / s_counters);
    }

}
