package main.java.myhbase;

import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ConcatenatedLists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hbase.HTableDescriptor;
/**
 * Created by lw_co on 2016/8/17.
 */

public class HBaseClientImpl {
    private static final Logger logger = LoggerFactory.getLogger(HBaseClientImpl.class);
    private static final Configuration config = HBaseConfiguration.create();
    private Table table;
    private Admin admin;

    public HBaseClientImpl(String tableName) throws IOException {
        Connection connection = ConnectionFactory.createConnection(config);
        table = connection.getTable(TableName.valueOf(tableName));
        admin = connection.getAdmin();
    }

    public byte[] getValue(String rowKey, String columnFamily, String qualifier) {
        Get get = new Get(Bytes.toBytes(rowKey));
        get.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier));

        Result result;
        try {
            result = table.get(get);
            return result.value();
        } catch (IOException e) {
            logger.error(e.getMessage());
        }

        return null;
    }

    public byte[] getValue(String rowKey, String columnFamily, String qualifier, long timeStamp) {
        Get get = new Get(Bytes.toBytes(rowKey));
        get.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier));

        Result result;
        try {
            get.setTimeStamp(timeStamp);
            result = table.get(get);
            return result.value();
        } catch (IOException e) {
            logger.error(e.getMessage());
        }

        return null;
    }

    public List<byte[]> getValues(String rowKey, String columnFamily, String qualifier,
                                  long minTimeStamp, long maxTimeStamp) {
        List<byte[]> results = new ArrayList<byte[]>();
        Result re = null;

        Get get = new Get(Bytes.toBytes(rowKey));
        get.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier));

        try {
            get.setTimeRange(minTimeStamp, maxTimeStamp);
            re = table.get(get);
        } catch (IOException e) {
            logger.error(e.getMessage());
            return results;
        }

        if(null != re) {
            for(Cell cell : re.listCells()) {
                byte[] value = new byte[cell.getValueLength()];
                System.arraycopy(cell.getValueArray(), cell.getValueOffset(),
                        value, 0, cell.getValueLength());
                results.add(value);
            }
        }

        return results;
    }
    public List<List<byte[]>> scanValues(String columnFamily, String qualifier) throws IOException {

        List<List<byte[]>> results = new ArrayList<List<byte[]>>();
        Scan scan=new Scan();
        scan.addColumn(Bytes.toBytes(columnFamily),Bytes.toBytes(qualifier));
        scan.setMaxVersions();//得到最大的版本数，这里得到1个版本，最新的，列族：限定词，version都确定了，所以得到的是一个唯一的值
        //scan.setStartRow(Bytes.toBytes("101010800-201605"));
        //scan.setStopRow(Bytes.toBytes("101010800-201607"));
        scan.setBatch(100);//返回最大的数目

        ResultScanner rs=table.getScanner(scan);//扫描得到的一个列表，每行为一个cell,而每个cell又是一个列表
        for(Result r:rs) {
            List<byte[]> rel = new ArrayList<byte[]>();
            rel.add(r.getRow());
            Cell cell = r.listCells().get(0);
            byte[] value = new byte[cell.getValueLength()];
            System.arraycopy(cell.getValueArray(), cell.getValueOffset(),
                    value, 0, cell.getValueLength());
            rel.add(value);
            //cells.toArray();
            results.add(rel);
        }
        rs.close();
        return results;
//        for(Result r:rs){
//            //List<Cell> val = r.getColumnCells(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier));
//            //results.add(r.getRow());//getRow返回的是rowkey
//            //因为字节是按字节存的，所以要存一个字符串必须是字节数组
//            List<byte[]> rel = new ArrayList<byte[]>();
//            for(Cell cell : r.listCells()) {
//                byte[] value = new byte[cell.getValueLength()];
//                System.arraycopy(cell.getValueArray(), cell.getValueOffset(),
//                        value, 0, cell.getValueLength());
//                rel.add(value);
//            }
//            //cells.toArray();
//            results.add(rel);
//        }

    }

    public boolean createTable(String tableName, String[] familys, int version) throws IOException {

    	/*创建的话，指定表明和列，关于列的限定符什么的以后增加数据的时候加，创建表的时候指定列族*/

        if (admin.tableExists(TableName.valueOf(tableName))) {
            System.out.println("table already exists!");
            return false;
        } else {
            HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(tableName));
            for(int i=0; i<familys.length; ++i){
                tableDesc.addFamily((new HColumnDescriptor(familys[i])).setMaxVersions(version));
            }
            admin.createTable(tableDesc);
            System.out.println("create table " + tableName + " ok.");
            return true;
        }
    }
    public void addRecord (String tableName, String rowKey, String family, String qualifier, String value)
            throws Exception{
        try {
            Connection connection = ConnectionFactory.createConnection(config);
            table = connection.getTable(TableName.valueOf(tableName));
            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes(family),Bytes.toBytes(qualifier),Bytes.toBytes(value));
            table.put(put);
            System.out.println("insert recored " + rowKey + " to table " + tableName +" ok.");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public void addRecords (String tableName, String rowKey, String family, String qualifier[], String value[])
            throws Exception{
        try {
            Connection connection = ConnectionFactory.createConnection(config);
            table = connection.getTable(TableName.valueOf(tableName));
            Put put = new Put(Bytes.toBytes(rowKey));
            for (int i=0;i<qualifier.length;++i)
            {
                put.addColumn(Bytes.toBytes(family),Bytes.toBytes(qualifier[i]),Bytes.toBytes(value[i]));

            }
            table.put(put);
            System.out.println("insert recored " + rowKey + " to table " + tableName +" ok.");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public void mapAddRecords (String tableName, String rowKey, String family, Map map)
            throws Exception{
        try {
            Connection connection = ConnectionFactory.createConnection(config);
            table = connection.getTable(TableName.valueOf(tableName));
            Put put = new Put(Bytes.toBytes(rowKey));
            Iterator it=map.entrySet().iterator();
            while(it.hasNext()){
                Entry<String,String> entry= (Entry<String, String>) it.next();
                put.addColumn(Bytes.toBytes(family),Bytes.toBytes(entry.getKey()),Bytes.toBytes(entry.getValue()));
            }
            table.put(put);
            System.out.println("insert recored " + rowKey + " to table " + tableName +" ok.");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

