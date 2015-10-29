package shell;

import com.constambeys.storm.DataFilter;
import hbase.Cons;
import jline.TerminalFactory;
import jline.console.ConsoleReader;
import hbase.Utils;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * Created by nickozoulis on 20/10/2015.
 */
public class Shell {

    static final Logger logger = LoggerFactory.getLogger(Shell.class);
    private static ConsoleReader console;

    public Shell() {
        Utils.setHBaseConfig();

        try {
            console = new ConsoleReader();
            console.setPrompt("serving_layer> ");

            String line;
            // Gets user's input
            while ((line = console.readLine()) != null) {
                parse(line.trim());
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            // Does some cleanup and restores the original terminal configuration.
            try {
                TerminalFactory.get().restore();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Decides what the user's input is about.
     *
     * @param line
     */
    private void parse(String line) {
        String[] splits = line.split(" ");
        switch (splits[0]) {
            case "get":
                parseGet(splits[1]);
                break;
            case "kmeans":
                parseKMeans(line, splits);
                break;
            case "test":
                Utils.testConnectionWithHBase(Utils.getHBaseConfig());
                break;
            case "create":
                Utils.createSchemaTables();
                break;
            case "scan":
                scanTable(line);
                break;
            case "man":
                displayManual();
                break;
            case "clear":
                clearScreen();
                break;
            case "exit":
                System.exit(0);
                break;
            default:
                usage();
                break;
        }
    }

    /**
     * Where the magic will happen.
     *
     * @param qid
     */
    private void parseGet(String qid) {
        Result batchClusters = Utils.getRowFromHTable(Cons.batch_views, qid);
        Result streamClusters = Utils.getRowFromHTable(Cons.stream_views, qid);

        // For the time being just print out both results, if available.
        printTemp(batchClusters, streamClusters);

        //TODO: Call fusion module to get fused results from batch and stream views.
        //TODO: Maybe implement iterator loop with for-each generics for the results

        /* example call
        Fusion f = new Fusion(batchClusters, streamClusters);

        for (String s : f.getClusters()) {
            System.out.println(s);
        }
        */
    }

    private void printTemp(Result batchClusters, Result streamClusters) {
        if (!batchClusters.isEmpty()) {
            System.out.println("---- Printing batch view ----");

            byte[] valueClusters = batchClusters.getValue(Bytes.toBytes(Cons.cfViews), Bytes.toBytes(Cons.clusters));
            if (valueClusters != null) {
                System.out.println(Bytes.toString(valueClusters));
            }
        } else
            System.out.println("Batch view is empty.");

        if (!streamClusters.isEmpty()) {
            System.out.println("---- Printing stream view ----");

            byte[] valueClusters = streamClusters.getValue(Bytes.toBytes(Cons.cfViews), Bytes.toBytes(Cons.clusters));
            if (valueClusters != null) {
                System.out.println(Bytes.toString(valueClusters));
            }
        } else
            System.out.println("Stream view is empty.");
    }

    private void clearScreen() {
        try {
            console.clearScreen();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void displayManual() {
        System.out.println("get [qid] -> Gets the views of the input query ID.");
        System.out.println("kmeans [numOfClusters] -> Inserts a kmeans query into HBase.");
        System.out.println("kmeans [numOfClusters] ; [filter] -> Inserts a constrained kmeans query into HBase.");
        System.out.println("test -> Tests the connection with HBase.");
        System.out.println("scan [tableName] [options: 0,1,2] -> Performs a scan of the input table.");
        System.out.println("create -> Create the schema tables of the HBase.");
        System.out.println("man -> Displays serving layer's manual.");
        System.out.println("clear -> Clears screen.");
    }

    private void scanTable(String line) {
        String pattern = "scan(\\s*)(\\w+)(\\s*)(\\d*)";
        Pattern r = Pattern.compile(pattern);
        Matcher m = r.matcher(line);

        if (m.find()) {
            String tableName = m.group(2), hTableName;
            String option = m.group(4);

            if (tableName.equalsIgnoreCase("queries"))
                hTableName = Cons.queries;
            else if (tableName.equalsIgnoreCase("batch"))
                hTableName = Cons.batch_views;
            else if (tableName.equalsIgnoreCase("stream"))
                hTableName = Cons.stream_views;
            else
                return;

            if (!option.equals("")) {
                try {
                    switch (hTableName) {
                        case Cons.queries:
                            Utils.scanQueriesHTable(Integer.parseInt(m.group(4)));
                            break;
                        case Cons.batch_views:
                            Utils.scanBatchHTable();
                            break;
                        case Cons.stream_views:
                            Utils.scanStreamHTable();
                            break;
                        default:
                            return;
                    }
                } catch (NumberFormatException e) {
                    System.out.println("Example usage: 'scan queries [0|1|2]");
                }
            } else {
                switch (hTableName) {
                    case Cons.queries:
                        Utils.scanQueriesHTable();
                        break;
                    case Cons.batch_views:
                        Utils.scanBatchHTable();
                        break;
                    case Cons.stream_views:
                        Utils.scanStreamHTable();
                        break;
                    default:
                        return;
                }
            }
        }
    }

    private void parseKMeans(String line, String[] splits) {
        if (splits.length == 2) { // Plain KMeans
            Utils.putQueryKMeans(splits[1]);
        } else if (splits.length > 2) { // Constrained KMeans
            parseConstraints(line);
        } else {
            usage();
        }
    }

    /**
     * Shows explanatory message when incorrect input is inserted.
     */
    private void usage() {
        try {
            console.println("Example input: 'kmeans 4' or 'kmeans 4 ; x1 + x2 < 6'");
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private void parseConstraints(String line) {
        String constraintExpr = "", clustersExpr = "", operator = "", numOfClusters = "";
        String pattern1 = "(.*);(.*)",
                pattern2 = "kmeans(\\s*)(\\d+)(\\s*)",
                pattern3 = "(==|!=|>=|<=|<|>)";

        // Get constraint
        Pattern r = Pattern.compile(pattern1);
        Matcher m = r.matcher(line);
        if (m.find()) {
            clustersExpr = m.group(1);
            constraintExpr = m.group(2);
        } else {
            usage();
            return;
        }

        // Get operator from constraint
        r = Pattern.compile(pattern3);
        m = r.matcher(line);
        if (m.find()) {
            operator = m.group(1);
        } else {
            usage();
            return;
        }

        // Get left and right expressions and create a DataFilter
        if (!operator.equals("")) {
            String splits[] = constraintExpr.split(operator);
            String leftExpr = parseExpression(splits[0].trim());
            String rightExpr = parseExpression(splits[1].trim());

            if (leftExpr.equals("") || rightExpr.equals("")) {
                return;
            }

            DataFilter filter = new DataFilter(leftExpr, operator, rightExpr);

            // Get numOfClusters
            r = Pattern.compile(pattern2);
            m = r.matcher(clustersExpr);
            if (m.find()) {
                numOfClusters = m.group(2);

                Utils.putQueryKMeansConstrained(numOfClusters, filter);
            }
        } else {
            usage();
            return;
        }
    }

    private String parseExpression(String str) {
        String expr = "";
        String pattern = "(\\w+)(((\\s+)(\\+|-|\\*|/)(\\s+)(\\w*))*)";
        Pattern r = Pattern.compile(pattern);
        Matcher m = r.matcher(str);

        if (m.matches()) {
            String splits[] = str.split(" ");

            expr += "{" + splits[0] + "}";

            if (splits.length >= 3) {
                for (int i = 1; i < splits.length; i += 2) {
                    expr += splits[i] + "{" + splits[i + 1] + "}";
                }
            }
        } else {
            usage();
        }

        return expr;
    }

    public static void main(String[] args) {
        new Shell();
    }

}
