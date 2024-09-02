package com.enzo.gmall.realtime.common.util;

import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
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
}
