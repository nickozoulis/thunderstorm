package experiment;

import filtering.Point;
import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by nickozoulis on 02/12/2015.
 */
public class DataSetReader implements Iterator<Point> {

    private int iter = 0;
    private List<Point> dataSet;

    public DataSetReader(File file) {
        dataSet = loadDataset(file);
    }

    public DataSetReader(List<Point> dataSet) {
        this.dataSet = dataSet;
    }

    private List<Point> loadDataset(File file) {
        List<Point> dataSet = new ArrayList<>((int)countLineNumber(file));

        try {
            BufferedReader br = new BufferedReader(new FileReader(file));

            String line;
            ArrayList<Double> ar = null;

            while ((line = br.readLine()) != null) {
                String[] splits = line.split("\\s+");

                boolean flag = true;
                ar = new ArrayList();
                for (int i = 0; i < 8; i++) {
                    try {
                        ar.add(Double.parseDouble(splits[i]));
                    } catch (NumberFormatException e) {
                        flag = false;
                    } catch (ArrayIndexOutOfBoundsException e) {
                        flag = false;
                    }
                }

                if (flag)
                    dataSet.add(new Point(ar.toArray(new Double[ar.size()])));
            }

            br.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return dataSet;
    }

    @Override
    public boolean hasNext() {
        return (iter < dataSet.size()) ? true : false;
    }

    @Override
    public Point next() {
        return dataSet.get(iter++);
    }

    public long countLineNumber(File file) {
        long lines = 0;
        try {

            LineNumberReader lineNumberReader = new LineNumberReader(
                    new FileReader(file));
            lineNumberReader.skip(Long.MAX_VALUE);
            lines = lineNumberReader.getLineNumber();
            lineNumberReader.close();

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return lines;
    }

    public void reset() {iter = 0;}

    public List<Point> getDataSet() {return this.dataSet;}
}
