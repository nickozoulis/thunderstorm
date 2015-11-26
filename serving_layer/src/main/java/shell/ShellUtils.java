package shell;

import clustering.KMeansQuery;
import clustering.LocalKMeans;
import clustering.QueryType;
import hbase.Cons;
import net.sf.javaml.core.Dataset;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by nickozoulis on 26/11/2015.
 */
public class ShellUtils {

    public static KMeansQuery parseKMeans(String line, String[] splits) {
        KMeansQuery query = null;

        if (splits.length == 2) { // Plain KMeans
            query = new KMeansQuery(Integer.parseInt(splits[1]));
        } else if (splits.length > 2) { // Constrained KMeans
            query = parseConstraints(line);
        }

        return query;
    }


    public static KMeansQuery parseConstraints(String line) {
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

    public static String kmeansUsage() {
        return "Example input: 'kmeans 4' or 'kmeans 4 ; x0+x1<6'";
    }

    public static void printResultView(Result result) {
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

    public static void printResultDataset(Dataset[] datasets) {
        for (Dataset d : datasets)
            System.out.println(d.instance(0));
    }

    /**
     * Performs the whole Lambda-KMeans procedure.
     *
     * @param query
     */
    public static void KMeans(KMeansQuery query) {
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
