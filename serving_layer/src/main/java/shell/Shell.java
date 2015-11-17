package shell;

import clustering.KMeansQuery;
import clustering.LocalKMeans;
import hbase.Cons;
import hbase.Utils;
import jline.TerminalFactory;
import jline.console.ConsoleReader;
import net.sf.javaml.core.Dataset;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
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
                getResultsFromViews(splits[1]);
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
    private void getResultsFromViews(String qid) {
        Result batchClusters = Utils.getRowFromBatchViews(Long.parseLong(qid));
        Result streamClusters = Utils.pollStreamViewForResult(Long.parseLong(qid));

        //FIXME: For the time being just print out both results, if available.
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
            System.out.println("< Printing batch view >");
            printResultView(batchClusters);
        } else
            System.out.println("> Batch view is empty <");

        if (!streamClusters.isEmpty()) {
            System.out.println("< Printing stream view >");
            printResultView(streamClusters);
        } else
            System.out.println("> Stream view is empty <");
    }

    private void printResultView(Result result) {
        byte[] valueClusters;
        int k = 0;

        for (;;) {
            valueClusters = result.getValue(Bytes.toBytes(Cons.cfViews), Bytes.toBytes(Cons.clusters_ + k));

            if (valueClusters != null)
                System.out.println(Bytes.toString(valueClusters));
            else
                break;

            k++;
        }
    }

    private void printResultDataset(Dataset[] datasets) {
        for (Dataset d : datasets)
            System.out.println(d.instance(0));
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
        KMeansQuery query = null;

        if (splits.length == 2) { // Plain KMeans
            query = new KMeansQuery(Integer.parseInt(splits[1]));
        } else if (splits.length > 2) { // Constrained KMeans
            query =  parseConstraints(line);
        } else {
            usage();
            return;
        }

        //FIXME
        if (query != null) KMeans(query);
    }

    /**
     * Shows explanatory message when incorrect input is inserted.
     */
    private void usage() {
        try {
            console.println("Example input: 'kmeans 4' or 'kmeans 4 ; x0+x1<6'");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private KMeansQuery parseConstraints(String line) {
        String constraintExpr = "", clustersExpr = "", operator = "", numOfClusters = "";
        String pattern1 = "(.*);(.*)",
                pattern2 = "kmeans(\\s*)(\\d+)(\\s*)";

        // Get constraint and clusters expressions
        Pattern r = Pattern.compile(pattern1);
        Matcher m = r.matcher(line);
        //FIXME: Make it work for more than one filter.
        if (m.find()) {
            clustersExpr = m.group(1);
            constraintExpr = m.group(2);
        } else {
            usage();
            return null;
        }

        // Get numOfClusters
        r = Pattern.compile(pattern2);
        m = r.matcher(clustersExpr);
        if (m.find()) {
            numOfClusters = m.group(2);

            Set set = new HashSet<>();
            set.add(constraintExpr);

            return new KMeansQuery(Integer.parseInt(numOfClusters), set);
        }

        return null;
    }

    public static void main(String[] args) {
        new Shell();
    }

    /**
     * Performs the whole Lambda-KMeans procedure.
     * @param query
     */
    private void KMeans(KMeansQuery query) {
        Result r = null;

        // Check if query exists in Queries table.
        long queryRowKey = Utils.getQueryIDIfExists(query);

        // If yes
        if (queryRowKey != -1) {
            // Check stream views if contain results for this query.
            r = Utils.getRowFromStreamViews(queryRowKey);

            // If yes, return it to the user.
            if (r != null) {
                printResultView(r);
                return;
            }

            // While these layers are computing, check whether there is a view for k'-means
            // (e.g., k'=10,000) for the same set of constraints
            KMeansQuery kQuery = new KMeansQuery(10000, query.getFilters()); //FIXME: move k' to Cons
            long kQueryRowKey = Utils.getQueryIDIfExists(kQuery);
            r = Utils.getRowFromStreamViews(kQueryRowKey);

            // If yes, then compute a Local k-out-of-k'-means clustering and return that to the user
            if (r != null) {
                printResultDataset(new LocalKMeans(query, Utils.loadClusters(r)).cluster());
                return;
            }

            // If no, send a {k' , {constraints}} query to both the streaming and batch layers via insertion to HBase.
            long newKey = Utils.putKMeansQuery(kQuery);
            // And start polling
            if (newKey != -1)
                r = Utils.pollStreamViewForResult(newKey);

        } else { // If no, put it in the HBase table and start polling.
            long newKey = Utils.putKMeansQuery(query);
            if (newKey != -1)
                r = Utils.pollStreamViewForResult(newKey);
        }

        printResultView(r);
    }

}
