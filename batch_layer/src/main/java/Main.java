import clustering.KMeansQuery;
import hbase.Cons;
import hbase.HBReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by nickozoulis on 18/11/2015.
 */
public class Main {

    static final Logger logger = LoggerFactory.getLogger(Main.class);
    private static long currentID = 1;

    public static void main(String[] args) throws InterruptedException {

        HBReader iter;

        for (;;) {
            iter = new HBReader(currentID);

            KMeansQuery kmQuery = iter.next();

            System.out.println("Starting spark kmeans for query: " + kmQuery);
            while (iter.hasNext()) {
                new SparkKMeans(kmQuery).run();
            }

            Thread.sleep(Cons.delay);
        }
    }

}
