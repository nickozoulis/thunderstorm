package experiment;

import filtering.Point;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * Created by nickozoulis on 02/12/2015.
 */
public class FileClusterReader implements Iterator<Point> {

    private Point[] ps;
    private int iter = 0;

    public FileClusterReader(String fileName) {
        String[] splits = fileName.split("_");

        //FIXME: Change how filenames are printed in shellutils
//        ps = new Point[Integer.parseInt(splits[1])];
        ps = new Point[5];

        try {
            BufferedReader br = new BufferedReader(new FileReader(fileName));

            String line = "";
            ArrayList<Double> ar;
            int i = 0;
            while ((line = br.readLine()) != null) {
                ar = new ArrayList();

                splits = line.split(",");
                for (String s : splits)
                    ar.add(Double.parseDouble(s));

                ps[i++] = new Point(ar.toArray(new Double[ar.size()]));
            }

            br.close();
        } catch (IOException e) {e.printStackTrace();}
    }

    @Override
    public boolean hasNext() {
        return (iter < ps.length) ? true : false;
    }

    @Override
    public Point next() {
        return ps[iter++];
    }

    public int getCluster(Point p) {
        int cluster = -1;
        double minDist = Double.MAX_VALUE;
        double dist;

        for (int i=0; i<ps.length; i++) {
            dist = Point.distance(ps[i], p);
            if (dist < minDist) {
                cluster = i;
                minDist = dist;
            }
        }

        return cluster;
    }
}
