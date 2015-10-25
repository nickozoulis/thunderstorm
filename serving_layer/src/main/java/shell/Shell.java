package shell;

import com.constambeys.storm.DataFilter;
import jline.TerminalFactory;
import jline.console.ConsoleReader;
import hbase.Utils;
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
            } catch(Exception e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Decides what the user's input is about.
     * @param line
     */
    private void parse(String line) {
        String[] splits = line.split(" ");
        switch(splits[0]) {
            case "kmeans":
                parseKMeans(line, splits);
                break;
            case "test":
                Utils.testConnectionWithHBase(Utils.getHBaseConfig());
                break;
            case "create":
                Utils.createSchemaTables();
                break;
            case "exit":
                System.exit(0);
                break;
            default:
                usage();
                break;
        }

    }

    private void parseKMeans(String line, String[] splits) {
        if (splits.length == 2) { // Plain KMeans
            Utils.queryKMeans(splits[1]);
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
        } catch (IOException e) {e.printStackTrace();}

    }

    private void parseConstraints(String line) {
        String constraint = "", operator = "", numOfClusters = "";
        String pattern1 = "(.*);(.*)",
                pattern2 = "kmeans (\\d)(\\s*);",
                pattern3 = "<|>|==|!=|>=|<=";

        // Get constraint
        Pattern r = Pattern.compile(pattern1);
        Matcher m = r.matcher(line);
        if (m.find()) {
            constraint = m.group(2);
        } else {
            usage();
            return;
        }

        // Get operator from constraint
        r = Pattern.compile(pattern3);
        m = r.matcher(line);
        if (m.find()) {
            operator = m.group(0);
        } else {
            usage();
            return;
        }

        // Get left and right expressions and create a DataFilter
        if (!operator.equals("")) {
            String splits[] = constraint.split(operator);
            String leftExpr = parseExpression(splits[0].trim());
            String rightExpr = parseExpression(splits[1].trim());

            if (leftExpr.equals("") || rightExpr.equals("")) { return; }

            DataFilter filter = new DataFilter(leftExpr, operator, rightExpr);

            // Get numOfClusters
            r = Pattern.compile(pattern2);
            m = r.matcher(line);
            if (m.find()) {
                numOfClusters = m.group(1);

                Utils.queryKMeansConstrained(numOfClusters, filter);
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
                for (int i=1; i<splits.length; i+=2) {
                    expr += splits[i] + "{" + splits[i+1] + "}";
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
