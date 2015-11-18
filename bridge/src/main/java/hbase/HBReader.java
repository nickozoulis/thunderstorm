package hbase;

import clustering.KMeansQuery;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by nickozoulis on 18/11/2015.
 */
public class HBReader implements Iterator<KMeansQuery> {

    static final Logger logger = LoggerFactory.getLogger(HBReader.class);
    private HConnection connection;
    private HTableInterface hTable;
    private long maxID = -1;
    private ResultScanner rs;
    private Result result;

    public HBReader(long currentID) {
        try {
            Configuration config = HBaseConfiguration.create();
            config.set("hbase.zookeeper.quorum", Cons.hbase_IP_address);
            config.set("hbase.zookeeper.property.clientPort", Cons.hbase_port);

            connection = HConnectionManager.createConnection(config);
            hTable = connection.getTable(Cons.queries);

            maxID = getMaxID();

            if (maxID != -1 && currentID <= maxID)
                rs = hTable.getScanner(new Scan(Bytes.toBytes(currentID), Bytes.toBytes(maxID)));
        } catch (IOException e) {e.printStackTrace();}
    }

    @Override
    public boolean hasNext() {
        if (rs != null) {
            try {
                if ((result = rs.next()) != null)
                    return true;
                else {
                    hTable.close();
                    connection.close();
                    return false;
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return false;
    }

    @Override
    public KMeansQuery next() {
        Result r = result;

        byte[] valueClusters = r.getValue(Bytes.toBytes(Cons.cfQueries), Bytes.toBytes(Cons.clusters));
        byte[] valueFilter = r.getValue(Bytes.toBytes(Cons.cfQueries), Bytes.toBytes(Cons.filter));

        KMeansQuery km = new KMeansQuery(Bytes.toLong(r.getRow()), Bytes.toInt(valueClusters));

        if (valueFilter != null) {
            String filter = Bytes.toString(valueFilter);
            km.getFilters().add(filter);
        }

        return km;
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
