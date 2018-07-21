import org.apache.hadoop.hbase.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;


import java.io.IOException;

public class HBaseCreateTable {

    private static Configuration conf = HBaseConfiguration.create();
    /**
     * 创建HBase的连接 必须需要配置文件
     *
     * 也可以手动设置　如下列格式
     *
     * configuration.set("hbase.zookeeper.property.clientPort", "2181");
     *
     *  设置远程连接的主机
     *  configuration.set("hbase.zookeeper.quorum", "192.168.1.21");
     *
     * configuration.set("hbase.master", "192.168.1.21:60000");
     *
      */



    private static Admin admin = null;

    private static Connection connection = null;

    public static void init() throws IOException {
        connection = ConnectionFactory.createConnection(conf);
        admin = connection.getAdmin();  // 得到admin　即就相当于操作HBase的句柄
    }

    public static void close() throws IOException {
        if(null!=admin){
            admin.close();
        }
        if(null != connection){
            connection.close();
        }
    }

    public static void createTable(String tableNmae, String[] cols) throws IOException {
        init();
        TableName tableName = TableName.valueOf(tableNmae); // 创建一个表
        if(admin.tableExists(tableName)){
            System.out.println("table is exists!");
        }
        else{
            HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName); // 创建一个表的实例
            for(String col: cols){
                HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(col);
                hTableDescriptor.addFamily(hColumnDescriptor);
            }
            admin.createTable(hTableDescriptor);
        }
        close();
    }

    // 删表

    public static void deleteTable(String tableName) throws IOException {
        init();
        TableName tn = TableName.valueOf(tableName);
        if(admin.tableExists(tn)){
            admin.disableTable(tn);
            admin.deleteTable(tn);
        }
        close();
    }

    // 查看已有表
    public static void listTables () throws IOException{
        init();
        HTableDescriptor [] hTableDescriptors = admin.listTables();
        for(HTableDescriptor hTableDescriptor: hTableDescriptors){
                System.out.println(hTableDescriptor.getNameAsString());
        }
    }

    public static void insertRow(String tableName, String rowkey, String colFamily, String col, String val) throws IOException {
        init();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes(rowkey));
        put.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(col), Bytes.toBytes(val));
        table.put(put);

        /*
         * 批量插入
         *
         * List <Put> putList = new ArrayList<Put>;
         *
         * 填充 putList
         *
         * table.put(putList);
         *
         */
    }

    public static void deleteRow(String tableName, String rowkey) throws IOException {
        init();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(Bytes.toBytes(rowkey));
        table.delete(delete);
        table.close();
        close();
    }

    public static void getDate(String tableName, String rowkey) throws IOException {
        init();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(rowkey));
        Result result = table.get(get);
        showCell(result);
    }

    // 格式化输出
    public static void showCell(Result result){
        Cell[] cells = result.rawCells();
        for (Cell cell : cells) {
            System.out.println("RowName:" + new String(CellUtil.cloneRow(cell)) + " ");
            System.out.println("Timetamp:" + cell.getTimestamp() + " ");
            System.out.println("column Family:" + new String(CellUtil.cloneFamily(cell)) + " ");
            System.out.println("row Name:" + new String(CellUtil.cloneQualifier(cell)) + " ");
            System.out.println("value:" + new String(CellUtil.cloneValue(cell)) + " ");
        }
    }

    public static void scanData(){

    }


    public static void main(String [] args) throws IOException {
        String [] cols = {"1", "2"};
        createTable("songjian", cols);
        listTables();
        insertRow("songjian", "first", "1", "first", "2");
//        deleteRow("songjian", "first");
        getDate("songjian", "first");
    }

}
