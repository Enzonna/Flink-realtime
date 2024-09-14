package com.enzo.gmall.realtime.common.util;

import com.alibaba.fastjson.JSONObject;
import com.enzo.gmall.realtime.common.constant.Constant;
import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;
import java.util.Set;

public class HBaseUtil {
    // 获取连接对象
    public static Connection getHBaseConnection() {
        try {
            Configuration conf = new Configuration();
            conf.set("hbase.zookeeper.quorum", "hadoop102,hadoop103,hadoop104");
            conf.set("hbase.zookeeper.property.clientPort", "2181");
            Connection connection = ConnectionFactory.createConnection(conf);
            return connection;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // 关闭连接对象
    public static void closeHBaseConnection(Connection connection) {
        if (connection != null) {
            try {
                connection.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }


    // 🍵🍵🍵获取异步操作连接对象
    public static AsyncConnection getHBaseAsyncConnection() {
        try {
            Configuration conf = new Configuration();
            conf.set("hbase.zookeeper.quorum", "hadoop102,hadoop103,hadoop104");
            AsyncConnection asyncConnection = ConnectionFactory.createAsyncConnection(conf).get();
            return asyncConnection;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // 🍵🍵🍵关闭异步操作连接对象
    public static void closeHBaseAsyncConnection(AsyncConnection asyncRedisConn) {
        if (asyncRedisConn != null && !asyncRedisConn.isClosed()) {
            try {
                asyncRedisConn.close();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }


    // 🍵🍵🍵以异步的方式根据rowkey到Hbase表中查询数据
    public static JSONObject readDimAsync(AsyncConnection asyncRedisConn, String nameSpace, String tableName, String rowKey) {
        try {
            TableName tableNameObj = TableName.valueOf(nameSpace, tableName);
            AsyncTable<AdvancedScanResultConsumer> asyncTable = asyncRedisConn.getTable(tableNameObj);
            Get get = new Get(Bytes.toBytes(rowKey));
            Result result = asyncTable.get(get).get();
            List<Cell> cells = result.listCells();
            if (cells != null && cells.size() > 0) {
                JSONObject dimJsonObj = new JSONObject();
                for (Cell cell : cells) {
                    String columnName = Bytes.toString(CellUtil.cloneQualifier(cell));
                    String columnValue = Bytes.toString(CellUtil.cloneValue(cell));
                    dimJsonObj.put(columnName, columnValue);
                }
                return dimJsonObj;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return null;
    }


    // 建表
    public static void createHBaseTable(Connection hbaseConn, String nameSpace, String tableName, String... families) {
        if (families.length < 1) {
            System.out.println("请输入至少一个列簇");
            return;
        }

        // 自动资源回收
        try (Admin admin = hbaseConn.getAdmin()) {
            // 判断表是否存在
            TableName tableNameObj = TableName.valueOf(nameSpace, tableName);
            if (admin.tableExists(tableNameObj)) {
                System.out.println(nameSpace + "下的" + tableName + "表已存在");
                return;
            }
            // 创建表描述器
            TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableNameObj);
            for (String family : families) {
                // 向表描述器中添加列簇
                tableDescriptorBuilder.setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(family)).build());
            }

            admin.createTable(tableDescriptorBuilder.build());
            System.out.println(nameSpace + "下的" + tableName + "创建成功✅✅");

        } catch (IOException e) {
            throw new RuntimeException(e);
        }


    }


    // 删表
    public static void dropHBaseTable(Connection hbaseConn, String nameSpace, String tableName) {
        try (Admin admin = hbaseConn.getAdmin()) {
            TableName tableNameObj = TableName.valueOf(nameSpace, tableName);
            if (!admin.tableExists(tableNameObj)) {
                System.out.println(nameSpace + "下的" + tableName + "表不存在❌❌");
                return;
            }
            admin.disableTable(tableNameObj);
            admin.deleteTable(tableNameObj);
            System.out.println(nameSpace + "下的" + tableName + "表成功✅✅");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    /**
     * put to HBase
     */
    public static void putRow(Connection hbaseConn, String nameSpace, String tableName, String rowKey, String family, JSONObject jsonObj) {
        TableName tableNameObj = TableName.valueOf(nameSpace, tableName);
        try (Table table = hbaseConn.getTable(tableNameObj)) {
            // put对象：封装rowKey、列簇、列名、列值
            Put put = new Put(Bytes.toBytes(rowKey));
            Set<String> columnNames = jsonObj.keySet();
            for (String columnName : columnNames) {
                String columnValue = jsonObj.getString(columnName);
                if (columnValue != null) {
                    put.addColumn(Bytes.toBytes(family), Bytes.toBytes(columnName), Bytes.toBytes(columnValue));
                }
            }
            table.put(put);
            System.out.println(nameSpace + "下的" + tableName + "表中put数据" + rowKey + "成功✅✅");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }


    // delete from HBase
    public static void deleteRow(Connection hbaseConn, String nameSpace, String tableName, String rowKey) {
        TableName tableNameObj = TableName.valueOf(nameSpace, tableName);
        try (Table table = hbaseConn.getTable(tableNameObj)) {
            Delete delete = new Delete(Bytes.toBytes(rowKey));
            table.delete(delete);
            System.out.println(nameSpace + "下的" + tableName + "表中delete数据" + rowKey + "成功✅✅");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    // 根据row，key到HBase表中查询一条数据出来
    public static <T> T getRow(Connection hbaseConn, String nameSpace, String tableName, String rowKey, Class<T> clazz, boolean isUnderTo) {

        TableName tableNameObj = TableName.valueOf(nameSpace, tableName);
        try (Table table = hbaseConn.getTable(tableNameObj)) {
            // 查询
            Result result = table.get(new Get(Bytes.toBytes(rowKey)));
            List<Cell> cells = result.listCells();
            if (cells != null && cells.size() > 0) {
                T obj = clazz.newInstance();
                for (Cell cell : cells) {
                    String columnName = Bytes.toString(CellUtil.cloneQualifier(cell));
                    String columnValue = Bytes.toString(CellUtil.cloneValue(cell));
                    if (isUnderTo) {
                        columnName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, columnName);
                    }
                    BeanUtils.setProperty(obj, columnName, columnValue);
                }
                return obj;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return null;
    }


    public static void main(String[] args) {
        Connection hBaseConnection = getHBaseConnection();
        JSONObject dim_base_trademark
                = getRow(hBaseConnection, Constant.HBASE_NAMESPACE, "dim_base_trademark", "1", JSONObject.class, false);
        System.out.println(dim_base_trademark);
        closeHBaseConnection(hBaseConnection);
    }
}
