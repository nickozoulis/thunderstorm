import clustering.KMeansQuery;
import hbase.Cons;
import hbase.HReaderScan;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by nickozoulis on 18/11/2015.
 */
public class Main {

	static final Logger logger = LoggerFactory.getLogger(Main.class);
	private static long currentID = 1;

	public static void main(String[] args) throws InterruptedException {
		try {
			HReaderScan iter = new HReaderScan(Cons.queries);

			for (;;) {
				iter.restart();

				KMeansQuery kmQuery;
				while ((kmQuery = iter.next()) != null) {

					System.out.println("Starting spark kmeans for query: " + kmQuery);

					new SparkKMeans(kmQuery).run();
				}

				Thread.sleep(Cons.batchDelay);
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

}
