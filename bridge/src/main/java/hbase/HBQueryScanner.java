package hbase;

import clustering.KMeansQuery;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Iterator;


/**
 * Created by nickozoulis on 19/11/2015.
 */

public class HBQueryScanner implements Iterator<KMeansQuery> {

    private static final Logger logger = Logger.getLogger(HBQueryScanner.class);
    private HConnection connection;
    private HTableInterface hTable;
    private long maxID = -1;
    private Iterator<Result> iterator;

    public HBQueryScanner(long currentID) {
        try {
            Configuration config = HBaseConfiguration.create();
            config.set("hbase.zookeeper.quorum", Cons.hbase_IP_address);
            config.set("hbase.zookeeper.property.clientPort", Cons.hbase_port);

            connection = HConnectionManager.createConnection(config);
            hTable = connection.getTable(Cons.queries);

            maxID = getMaxID();
            ResultScanner rs;

            if (maxID != -1 && currentID <= maxID) {
                rs = hTable.getScanner(new Scan(Bytes.toBytes(currentID), Bytes.toBytes(maxID+1)));
                iterator = rs.iterator();
            }

        } catch (IOException e) {e.printStackTrace();}
    }

    @Override
    public boolean hasNext() {
        if (iterator != null)
            return iterator.hasNext() ? true : false;
        else
            return false;
    }

    @Override
    public KMeansQuery next() {
        Result r = iterator.next();

        byte[] valueClusters = r.getValue(Bytes.toBytes(Cons.cfQueries), Bytes.toBytes(Cons.clusters));
        byte[] valueFilter = r.getValue(Bytes.toBytes(Cons.cfQueries), Bytes.toBytes(Cons.filter));

        KMeansQuery km = new KMeansQuery(Bytes.toLong(r.getRow()), Bytes.toInt(valueClusters));

        if (valueFilter != null) {
            String filter = Bytes.toString(valueFilter);
            km.getFilters().add(filter);
        }

        return km;
    }

    public void closeHBConnection() {
        try {
            hTable.close();
            connection.close();
        } catch (IOException e) {e.printStackTrace();}
    }

    private long getMaxID() throws IOException {
        Get g;
        Result r;

        g = new Get(Bytes.toBytes(0l));
        r = hTable.get(g);

        byte[] value = r.getValue(Bytes.toBytes(Cons.cfQueries), Bytes.toBytes(Cons.max_qid));
        return Bytes.toLong(value);
    }


}
