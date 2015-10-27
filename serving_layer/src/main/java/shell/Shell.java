package shell;

import com.constambeys.storm.DataFilter;
import jline.TerminalFactory;
import jline.console.ConsoleReader;
import hbase.Utils;
import org.apache.hadoop.hbase.client.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
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
        //TODO: Call fusion module to get fused results from batch and stream views.
        //TODO: Implement iterator loop with for-each generics for the results
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
        System.out.println("scan [tableName] -> Performs a scan of the input table.");
        System.out.println("create -> Create the schema tables of the HBase.");
        System.out.println("man -> Displays serving layer's manual.");
    }

    private void scanTable(String line) {
        String pattern = "scan(\\s*)(\\w+)(\\s*)(\\d*)";
        Pattern r = Pattern.compile(pattern);
        Matcher m = r.matcher(line);

        if (m.find()) {
            String tableName = m.group(2);
            String option = m.group(4);

            if (!option.equals("")) {
                try {
                    Utils.scanTable(tableName, Integer.parseInt(m.group(4)));
                } catch (NumberFormatException e) {
                    System.out.println("Example usage: 'scan queries [0|1|2]");
                }
            } else {
                Utils.scanTable(tableName, 0);
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
