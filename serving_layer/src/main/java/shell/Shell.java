package shell;

import experiment.ExperimentDriver;
import hbase.Cons;
import hbase.Utils;
import jline.TerminalFactory;
import jline.console.ConsoleReader;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import org.apache.hadoop.hbase.client.Result;
import org.apache.log4j.Logger;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * Created by nickozoulis on 20/10/2015.
 */
public class Shell {

    private static final Logger logger = Logger.getLogger(Shell.class);
    private static ConsoleReader console;

    public Shell() {
        HBaseUtils.setHBaseConfig();
        Utils.initSparkContext();

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

    public Shell(String file) {
        Utils.initSparkContext();
        List<String> queryList = new ArrayList<>();

        try {
            BufferedReader br = new BufferedReader(new FileReader(file));
            String line;

            while ((line = br.readLine()) != null) {
                queryList.add(line);
            }
        } catch (IOException e) {e.printStackTrace();}

        new ExperimentDriver(queryList);
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
                ShellUtils.KMeans(ShellUtils.parseKMeans(line, splits));
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
                try {
                    console.println(ShellUtils.kmeansUsage());
                } catch (IOException e) {e.printStackTrace();}
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
                    ShellUtils.printResultView(r);
                else
                    System.out.println("No batch view for qid: " + qid);
                break;
            case "stream":
                r = HBaseUtils.getRowFromStreamViews(qid);
                if (r != null)
                    ShellUtils.printResultView(r);
                else
                    System.out.println("No stream view for qid: " + qid);
                break;
            default:
                System.err.println("Example use: get [batch/stream] [qid]");
                break;
        }
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

    public static void main(String[] args) {
        OptionParser parser = new OptionParser("f:");
        OptionSet options = parser.parse(args);

        if (options.hasArgument("f")) { // Experiment mode
            new Shell(options.valueOf("f").toString());
        } else { // Normal mode
            new Shell();
        }
    }

}
