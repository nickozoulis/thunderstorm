package shell;

import jline.TerminalFactory;
import jline.console.ConsoleReader;
import utils.Utilities;

import java.io.IOException;


/**
 * Created by nickozoulis on 20/10/2015.
 */
public class Shell {
    
    private static ConsoleReader console;

    public Shell() {
        try {
            console = new ConsoleReader();
            console.setPrompt("serving_layer> ");

            String line = null;
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
     * Desides what the user's input is about.
     * @param line
     */
    private void parse(String line) {
        String[] splits = line.split(" ");
        switch(splits[0]) {
            case "kmeans":
                parseKMeans(line, splits);
                break;
            default:
                usage();
                break;
        }

    }

    private void parseKMeans(String line, String[] splits) {
        if (splits.length == 2) { // Plain KMeans
            Utilities.queryKMeans(splits[1]);
        } else if (splits.length > 2) { // Constrained KMeans
            //FIXME
            Utilities.queryKMeansConstrained(splits[1], "");
        } else {
            usage();
        }
    }

    /**
     * Shows explanatory message when incorrect input is inserted.
     */
    private void usage() {
        try {
            console.println("Usage example: kmeans [numOfClusters] or kmeans [numOfClusters] [constraints]");
        } catch (IOException e) {e.printStackTrace();}

    }

    public static void main(String[] args) {
        new Shell();
    }

}
