import hbase.Cons;
import hbase.Utils;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

/**
 * Created by nickozoulis on 25/11/2015.
 */
public class Main {

    static final Logger logger = LoggerFactory.getLogger(Main.class);
    private static String filePath;
    private static int delay, range;
    private static HConnection connection;


    public static void main(String[] args) {
        // Initialize connection with HBase
        connection = Utils.initHBaseConnection();
        if (connection == null) {
            System.err.println("No connection with HBase.");
            System.exit(1);
        }

        OptionParser parser = new OptionParser("f:d:r:");
        OptionSet options = parser.parse(args);

        if (options.hasArgument("f")) {
            filePath = options.valueOf("f").toString();
        }
        if (options.hasArgument("d")) {
            delay = 1000 * Integer.parseInt(options.valueOf("d").toString());   // seconds to milliseconds
        }
        if (options.hasArgument("r")) {
            range = Integer.parseInt(options.valueOf("r").toString());
        }

        try {
            HTableInterface hTable = connection.getTable(Cons.raw_data);

            BufferedReader br = new BufferedReader(new FileReader(filePath));
            String line;

            long counter = 0;
            while ((line = br.readLine()) != null) {
                String[] splits = line.split("\\s+");

                Put p = new Put(Bytes.toBytes(counter++));

                int numOfAttr = 0;
                double attr;
                boolean flag = true;
                for (int i=11; i<=18; i++) {
                    try {
                        attr = Double.parseDouble(splits[i]);
                        p.add(Bytes.toBytes(Cons.cfAttributes),
                                Bytes.toBytes(numOfAttr++), Bytes.toBytes(attr));
                    } catch(NumberFormatException e) {
                        e.printStackTrace();
                        flag = false;
                    } catch(ArrayIndexOutOfBoundsException e) {
                        e.printStackTrace();
                        flag = false;
                    }
                }

                // A column will contain the number of attributes contained in this row.
                p.add(Bytes.toBytes(Cons.cfAttributes),
                        Bytes.toBytes(Cons.numOfAttr), Bytes.toBytes(numOfAttr));

                // If row is correct, put it in HBase.
                if (flag) hTable.put(p);

                // Delay
                if (counter % range == 0) {
                    try {
                        Thread.sleep(delay);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }

            hTable.close();
            connection.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
