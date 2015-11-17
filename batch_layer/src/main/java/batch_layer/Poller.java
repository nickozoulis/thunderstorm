import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.util.Bytes;
//import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;



public class Poller {
	public static final String HBASEHOST = "hbase.master";
	public static final String HBASEHOSTVALUE = Cons.hbase_IP_address + ":" + Cons.hbase_port;
	public static final String ROWPREFIX = Cons.qid_;
	public static final String COLUMNFAMILY = Cons.cfQueries;
	public static final String COLUMNQUALIFIER = "a";
	
	public static int time = 0;
	public static final int PERIOD = 10;
	public static int last_row = Integer.parseInt(Cons.qid_0.split("_")[1]);
	public static final int FIRSTROW = 1;
	//public static int last_printed = 0;
	public static final int POLLDURATION = 1;
	
	public static void main(String[] args) throws IOException, InterruptedException {
		
		Configuration conf = HBaseConfiguration.create();
		conf.set(HBASEHOST, HBASEHOSTVALUE);
		HConnection connection = HConnectionManager.createConnection(conf);
		HTableInterface table = connection.getTable(Cons.queries);
		
		
		while (true){
			time++;
			if (time == PERIOD) {
				System.out.println("all queries in queue");
				Scan scan = new Scan();
				scan.addFamily(Bytes.toBytes(Cons.cfQueries));
				ResultScanner scanner = table.getScanner(scan);
				for (Result result = scanner.next(); (result != null); result = scanner.next()) {
					for(Cell cell : result.listCells()) {
						String value = Bytes.toString(CellUtil.cloneValue(cell));
					    System.out.println(value);
					}
				}
				time=0;
			}
			else{
				System.out.println("last queries in queue");
				Scan scan = new Scan(Bytes.toBytes(ROWPREFIX + last_row));
				scan.addFamily(Bytes.toBytes(Cons.cfQueries));
				ResultScanner scanner = table.getScanner(scan);
				for (Result result = scanner.next(); (result != null); result = scanner.next()) {
					for(Cell cell : result.listCells()) {
						String value = Bytes.toString(CellUtil.cloneValue(cell));
						last_row++;
					    System.out.println(value);
					}
				}
				
				
				
			}
			//last = last_printed;
			Thread.sleep(POLLDURATION);
		}
	}

}
