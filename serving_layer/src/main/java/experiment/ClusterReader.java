package experiment;

import filtering.Point;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * Created by nickozoulis on 02/12/2015.
 */
public class ClusterReader implements Iterator<Point> {

    private Point[] clusterHeads;
    private int iter = 0;
    private File file;

    public ClusterReader(File file) {
        this.file = file;
        clusterHeads = loadClusterHeads(file);
    }

    private Point[] loadClusterHeads(File file) {
        String[] splits = file.getName().split("_");
        Point[] clusterHeads = new Point[Integer.parseInt(splits[1])];

        try {
            BufferedReader br = new BufferedReader(new FileReader(file));

            String line;
            ArrayList<Double> ar;
            int i = 0;
            while ((line = br.readLine()) != null) {
                ar = new ArrayList();

                splits = line.split(",");
                for (String s : splits)
                    ar.add(Double.parseDouble(s));

                clusterHeads[i++] = new Point(ar.toArray(new Double[ar.size()]));
            }

            br.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return clusterHeads;
    }

    @Override
    public boolean hasNext() {
        return iter < clusterHeads.length;
    }

    @Override
    public Point next() {
        return clusterHeads[iter++];
    }

    public int getCluster(Point p) {
        int cluster = -1;
        double minDist = Double.MAX_VALUE;
        double dist;

        for (int i = 0; i < clusterHeads.length; i++) {
            dist = Point.distance(clusterHeads[i], p);
            if (dist < minDist) {
                cluster = i;
                minDist = dist;
            }
        }

        return cluster;
    }

    public int getNearestCluster(int c) {
        Point p = clusterHeads[c];
        int nearestCluster = c;
        double minDist = Double.MAX_VALUE;
        double dist;

        for (int i=0; i<clusterHeads.length; i++) {
            if (!p.equals(clusterHeads[i])) {
                dist = Point.distance(p, clusterHeads[i]);
                if (dist < minDist) {
                    nearestCluster = i;
                    minDist = dist;
                }
            }
        }

        return nearestCluster;
    }


    public File getFile() {return this.file;}

}
