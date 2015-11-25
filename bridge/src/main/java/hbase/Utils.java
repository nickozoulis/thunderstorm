package hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * Created by nickozoulis on 20/10/2015.
 */
public class Utils {

    private static final Logger logger = Logger.getLogger(Utils.class);

    public static HConnection initHBaseConnection() {
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

}
