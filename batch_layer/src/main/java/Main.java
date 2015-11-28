import clustering.KMeansQuery;
import hbase.Cons;
import hbase.HBQueryScanner;
import hbase.HMessages;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.jcraft.jsch.jce.HMACMD5;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

/**
 * Created by nickozoulis on 18/11/2015.
 */
public class Main {
	// Execute jar as: java -cp batch_layer-1.0-SNAPSHOT-allinone.jar Main
	private static final Logger logger = Logger.getLogger(Main.class);
	private static long currentID = 1, start = 0, end = Long.MAX_VALUE;
	private static String tableName = Cons.raw_data;
	private static HConnection connection;
	private static JavaSparkContext sc;

	public static void main(String[] args) throws Exception {
		// Initialize connection with HBase
		connection = initHBaseConnection();
		if (connection == null)
			System.exit(1);

		// Get argument values, if they exist, or use the default ones.
		OptionParser parser = new OptionParser("t:s:e:");
		OptionSet options = parser.parse(args);

		if (options.hasArgument("t")) {
			tableName = options.valueOf("t").toString();
		}
		if (options.hasArgument("s")) {
			start = Long.parseLong(options.valueOf("s").toString());
		}
		if (options.hasArgument("e")) {
			end = Long.parseLong(options.valueOf("e").toString());
		}

		// Set Spark configuration
		SparkConf sparkConf = new SparkConf();
		sparkConf.setAppName("SparKMeans");
		sparkConf.setMaster("local");
		sc = new JavaSparkContext(sparkConf);

		// Initialize a thread pool for SparkKMeans threads
		ScheduledExecutorService ex = new ScheduledThreadPoolExecutor(10);
		Collection<Future<?>> futures = new LinkedList<>();

		HBQueryScanner iterator;
		JavaRDD<String> dataSet;

		for (;;) {
			// Load DataSet
			dataSet = loadDataSetFromHBase(tableName, start, end);
			// Foreach Query execute SparkKMeans
			iterator = new HBQueryScanner(currentID);

			while (iterator.hasNext()) {
				KMeansQuery kmQuery = iterator.next();

				logger.info("Starting spark kmeans for query: " + kmQuery);

				futures.add(ex.submit(new SparkKMeans(dataSet, kmQuery)));
            }

			iterator.closeHBConnection();

			for (Future<?> future : futures) {
				future.get();
			}
			futures.clear();

			HMessages m = new HMessages(Cons.messages);
			m.write(0, m.read_long(0) + 1);
			m.close();

			logger.info("Batch layer going to sleep for + " + Cons.batchDelay + " ms");
			Thread.sleep(Cons.batchDelay);
		}

	}

	private static HConnection initHBaseConnection() {
		try {
			Configuration config = HBaseConfiguration.create();
			config.set("hbase.zookeeper.quorum", Cons.hbase_IP_address);
			config.set("hbase.zookeeper.property.clientPort", Cons.hbase_port);

			return HConnectionManager.createConnection(config);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	private static JavaRDD<String> loadDataSetFromHBase(String tableName, long start, long end) {
		List<String> dataSet = new ArrayList<>((int) ((end + 1) - start));
		try {
			HTableInterface hTable = connection.getTable(tableName);

			ResultScanner rs = hTable.getScanner(new Scan(Bytes.toBytes(start), Bytes.toBytes(end + 1)));
			int numOfAttr = 0;
			for (Result r : rs) {
				byte[] value = r.getValue(Bytes.toBytes(Cons.cfAttributes), Bytes.toBytes(Cons.numOfAttr));
				numOfAttr = Bytes.toInt(value);

				// Foreach result, form a String containing each attribute separated by comma.
				String s = "";
				for (int i = 0; i < numOfAttr; i++) {
					value = r.getValue(Bytes.toBytes(Cons.cfAttributes), Bytes.toBytes(i));

					double attr = Bytes.toDouble(value);
					s += attr + ",";
				}
				s = s.substring(0, s.length() - 1); // remove last comma
				dataSet.add(s);
			}

			return sc.parallelize(dataSet, numOfAttr);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

}
