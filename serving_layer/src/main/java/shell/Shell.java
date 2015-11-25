package shell;

import clustering.KMeansQuery;
import clustering.LocalKMeans;
import clustering.QueryType;
import hbase.Cons;
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
        HBaseUtils.setHBaseConfig();

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
                get(splits);
                break;
            case "kmeans":
                parseKMeans(line, splits);
                break;
            case "test":
                HBaseUtils.testConnectionWithHBase(HBaseUtils.getHBaseConfig());
                break;
            case "create":
                HBaseUtils.createSchemaTables();
                break;
            case "scan":
                scanTable(line);
                break;
            case "put":
                HBaseUtils.insertRawData(splits);
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


    private void get(String[] splits) {
        long qid;
        try {
            qid = Long.parseLong(splits[2]);
        } catch (NumberFormatException e) {
            System.err.println("Example use: get [batch/stream] [qid]");
            return;
        }

        Result r;
        switch (splits[1]) {
            case "batch":
                r = HBaseUtils.getRowFromBatchViews(qid);
                if (r != null)
                    printResultView(r);
                else
                    System.out.println("No batch view for qid: " + qid);
                break;
            case "stream":
                r = HBaseUtils.getRowFromStreamViews(qid);
                if (r != null)
                    printResultView(r);
                else
                    System.out.println("No stream view for qid: " + qid);
                break;
            default:
                System.err.println("Example use: get [batch/stream] [qid]");
                break;
        }
    }

    private void printResultView(Result result) {
        byte[] valueClusters;
        int k = 0;

        System.out.println();
        for (;;) {
            valueClusters = result.getValue(Bytes.toBytes(Cons.cfViews), Bytes.toBytes(Cons.clusters_ + k));

            if (valueClusters != null)
                System.out.println(Bytes.toString(valueClusters));
            else
                break;

            k++;
        }
        System.out.println();
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
        System.out.println("get [batch/stream] [qid] -> Gets the views of the input query ID from the specified table.");
        System.out.println("kmeans [numOfClusters] (; [filter]) -> Inserts a (constrained) KMeans query into HBase.");
        System.out.println("put [absolute file path] -> Inserts data to the raw_data table.");
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
                            HBaseUtils.listQueriesHTable(Integer.parseInt(m.group(4)));
                            break;
                        case Cons.batch_views:
                            HBaseUtils.listBatchHTable();
                            break;
                        case Cons.stream_views:
                            HBaseUtils.listStreamHTable();
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
                        HBaseUtils.listQueriesHTable();
                        break;
                    case Cons.batch_views:
                        HBaseUtils.listBatchHTable();
                        break;
                    case Cons.stream_views:
                        HBaseUtils.listStreamHTable();
                        break;
                    default:
                        return;
                }
            }
        }
    }

    private void parseKMeans(String line, String[] splits) {
        KMeansQuery query;

        if (splits.length == 2) { // Plain KMeans
            query = new KMeansQuery(Integer.parseInt(splits[1]));
        } else if (splits.length > 2) { // Constrained KMeans
            query = parseConstraints(line);
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
     *
     * @param query
     */
    private void KMeans(KMeansQuery query) {
        Result r;

        // Check if query exists in Queries table.
        long queryRowKey = HBaseUtils.getQueryIDIfExists(query);

        // If no, add it to HBase
        if (queryRowKey == -1)
            HBaseUtils.putKMeansQuery(query);

        // Check stream views if contain results for this query. Assuming stream takes batch views as input.
        r = HBaseUtils.getRowFromStreamViews(queryRowKey);

        // If yes, return it to the user.
        if (!r.isEmpty()) {
            printResultView(r);
            return;
        }

        /*
            While these layers are computing, check whether there is a view for k'-means
            (e.g., k'=10,000) for the same set of constraints
        */
        KMeansQuery kQuery = null;
        if (query.getQueryType() == QueryType.KMEANS)
            kQuery = new KMeansQuery(Cons.K);
        else if (query.getQueryType() == QueryType.CONSTRAINED_KMEANS)
            kQuery = new KMeansQuery(Cons.K, query.getFilters());
        // Check whether this kQuery already exists

        long kQueryRowKey = HBaseUtils.getQueryIDIfExists(kQuery);

        // If no, add it to HBase
        if (kQueryRowKey == -1)
            HBaseUtils.putKMeansQuery(kQuery);

        // Check stream views if contain results for this kQuery.
        r = HBaseUtils.getRowFromStreamViews(kQueryRowKey);

        // If yes, then compute a Local k-out-of-k'-means clustering and return that to the user
        if (!r.isEmpty()) {
            printResultDataset(new LocalKMeans(query, HBaseUtils.loadClusters(r)).cluster());
            return;
        }


        /*
            While they are computing, we retrieve the k'-means view for the whole dataset (i.e., no constraints),
            and compute and return a k-out-of-k'-means result to the user.
        */

    }

}
