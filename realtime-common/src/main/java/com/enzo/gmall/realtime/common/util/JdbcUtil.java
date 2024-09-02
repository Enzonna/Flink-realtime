package com.enzo.gmall.realtime.common.util;

import com.enzo.gmall.realtime.common.constant.Constant;
import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * ✅✅✅通用Jdbc方法
 */
public class JdbcUtil {

    public static Connection getMySqlConnection() throws Exception {
        // Register the driver
        Class.forName(Constant.MYSQL_DRIVER);

        // Get connected
        Connection connection = DriverManager.getConnection(Constant.MYSQL_URL, Constant.MYSQL_USER_NAME, Constant.MYSQL_PASSWORD);

        return connection;
    }

    public static void closeMySqlConnection(Connection conn) throws Exception {
        if (conn != null && !conn.isClosed()) {
            conn.close();
        }
    }

    public static <T> List<T> queryList(Connection conn, String sql, Class<T> clz) throws Exception {
        return queryList(conn, sql, clz, false);
    }

    // 从MySql数据库表中查询数据
    public static <T> List<T> queryList(Connection conn, String sql, Class<T> clz, boolean underlineToCamel) throws Exception {
        List<T> reList = new ArrayList<>();
        // Create a database object
        PreparedStatement ps = conn.prepareStatement(sql);

        // execute sql
        ResultSet rs = ps.executeQuery();
        ResultSetMetaData metaData = rs.getMetaData();

        // Process result set
        while (rs.next()) {
            // Create an object to receive the result from a query
            T obj = clz.newInstance();
            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                String columnName = metaData.getColumnName(i);
                Object columnValue = rs.getObject(i);
                // Convert column name to camel case
                if (underlineToCamel) {
                    columnName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, columnName);
                }
                BeanUtils.setProperty(obj, columnName, columnValue);
            }
            reList.add(obj);
        }

        // Free resources
        rs.close();
        ps.close();

        return reList;
    }
}
