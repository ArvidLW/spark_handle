package main.java.myhbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;

/**
 * Created by lw_co on 2016/8/23.
 */
public class ConnectHBase {
    //private static final Logger logger = LoggerFactory.getLogger(HBaseClientImpl.class);
    private static final Configuration config = HBaseConfiguration.create();
    private Table table;
    private Admin admin;

    public ConnectHBase(String tableName) throws IOException {
        Connection connection = ConnectionFactory.createConnection(config);
        table = connection.getTable(TableName.valueOf(tableName));
        admin = connection.getAdmin();
    }
}
