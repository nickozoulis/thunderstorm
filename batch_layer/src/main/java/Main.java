import clustering.KMeansQuery;
import hbase.Cons;
import hbase.HBQueryScanner;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by nickozoulis on 18/11/2015.
 */
public class Main {

	static final Logger logger = LoggerFactory.getLogger(Main.class);
	private static long currentID = 1;

	public static void main(String[] args) throws InterruptedException {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("JavaKMeans");
        sparkConf.setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

		try {
			HBQueryScanner iterator;

			for (;;) {
                iterator = new HBQueryScanner(currentID);

				while (iterator.hasNext()) {
                    KMeansQuery kmQuery = iterator.next();

					System.out.println("Starting spark kmeans for query: " + kmQuery);

					new SparkKMeans(sc, kmQuery).run();
				}
                iterator.closeHBConnection();
				Thread.sleep(Cons.batchDelay);
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

}
