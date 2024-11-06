
package com.huawei.bigdata.hive;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * JDBC客户端
 * 通过Jdbc方式解析删除的脚本,并把需要删除的数据插入到临时表当中
 *
 * @author
 * @since 8.0.0
 */
public class Client {
    private static final Logger logger = LoggerFactory.getLogger(Client.class);
    private static final String HIVE_DRIVER = "org.apache.hive.jdbc.HiveDriver";

    private static final String ZOOKEEPER_SERVER_PRINCIPAL_KEY = "zookeeper.server.principal";
    private static String ZOOKEEPER_DEFAULT_SERVER_PRINCIPAL = null;

    private static String KRB5_FILE = null;

    /* zookeeper认证方式 */
    private static String auth = null;

    // 初始化参数
    private static void init() throws IOException {
        System.out.println("初始化配置文件...........");
        // 客户端配置文件
        Properties clientInfo = null;
        // 寻找当前用户目录下的配置文件
        String userdir = System.getProperty("user.dir")
                + File.separator
                + "src"
                + File.separator
                + "main"
                + File.separator
                + "resources"
                + File.separator
                + "Kerberos"
                + File.separator;
        InputStream fileInputStream = null;
        try {
            clientInfo = new Properties();
            /**
             * "hiveclient.properties"为客户端配置文件，如果使用多实例特性，需要把该文件换成对应实例客户端下的"hiveclient.properties"
             * "hiveclient.properties"文件位置在对应实例客户端安裝包解压目录下的config目录下
             */
            String hiveclientProp = userdir + "hiveclient.properties";

            File propertiesFile = new File(hiveclientProp);
            fileInputStream = new FileInputStream(propertiesFile);
            clientInfo.load(fileInputStream);
        } catch (IOException e) {
            throw new IOException(e);
        } finally {
            if (fileInputStream != null) {
                fileInputStream.close();
            }
        }

        auth = clientInfo.getProperty("auth");

        // String krb5Path = userdir + "krb5.conf";
        // String krb5Path = resourceLoader.getResource("krb5.conf").getPath();
        KRB5_FILE = userdir + "krb5.conf";
        System.setProperty("java.security.krb5.conf", KRB5_FILE);

        if ("KERBEROS".equalsIgnoreCase(auth)) {
            // 设置客户端的keytab和zookeeper认证principal

            ZOOKEEPER_DEFAULT_SERVER_PRINCIPAL = "zookeeper/hadoop.hadoop.com";
            System.setProperty(ZOOKEEPER_SERVER_PRINCIPAL_KEY, ZOOKEEPER_DEFAULT_SERVER_PRINCIPAL);
        }

        // zookeeper开启ssl时需要设置JVM参数
        // LoginUtil.processZkSsl(clientInfo);
    }

    /**
     * @throws ClassNotFoundException
     * @throws SQLException
     * @throws IOException
     */
    @SuppressWarnings("rawtypes")
    public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException {

        // 参数初始化
        init();

        String url = "jdbc:hive2://10.97.213.6:24002,10.97.213.5:24002,10.97.213.4:24002/;serviceDiscoveryMode=zooKeeper;mapreduce.job.queuename=bigdata_prd;zooKeeperNamespace=hiveserver2;sasl.qop=auth-conf;auth=KERBEROS;principal=hive/hadoop.hadoop.com@HADOOP.COM;user.principal=W0008818;user.keytab=src/main/resources/Kerberos/user.keytab;";
        // 输出拼接的URL
        System.out.println("----------------------------------------------------------------------");
        System.out.println("************拼接好的URL************\n" + url);
        System.out.println("----------------------------------------------------------------------");
        // 加载Hive JDBC驱动
        Class.forName(HIVE_DRIVER);
        // 创建JDBC连接
        Connection connection = null;
        try {
            // 获取JDBC连接
            // 如果使用的是普通模式，那么第二个参数需要填写正确的用户名，否则会以匿名用户(anonymous)登录
            connection = DriverManager.getConnection(url, "", "");
            // 执行DDL任务
            Statement stmt = connection.createStatement(); // 创建Statement对象来执行SQL查询
            ResultSet rs = stmt.executeQuery(
                    "SELECT yearmonth AS A_yearmonth, COUNT(1) AS B_Totall, SUM(AMT_ORDER_SUM) AS C_AMT_ORDER_SUM, SUM(QTY_DEMAND_SUM1) AS D_QTY_DEMAND_SUM, SUM(QTY_ORDER_SUM1) AS E_QTY_ORDER_SUM\r\n"
                            + //
                            "FROM sdi.sdi_hy_cc_order\r\n" + //
                            "WHERE yearmonth >= '202301'\r\n" + //
                            "GROUP BY yearmonth\r\n" + //
                            "ORDER BY yearmonth ASC"); // 执行查询并获取结果
            // 处理结果
            List list = getList(rs);
            System.out.println("----------------------------------------------------------------------");
            System.out.println("************查询结果************");
            for (Object object : list) {
                System.out.println(object);
            }
            System.out.println("----------------------------------------------------------------------");
            // 关闭结果集、声明和连接
            rs.close();
            stmt.close();
            connection.close();

        } catch (SQLException e) {
            logger.error("Create connection failed : " + e.getMessage());
        } finally {
            // 关闭JDBC连接
            if (null != connection) {
                connection.close();
            }
        }
    }

    // 获取查询结果
    @SuppressWarnings({ "rawtypes", "unchecked" })
    private static List getList(ResultSet rs) throws SQLException {
        List list = new ArrayList();

        ResultSetMetaData md = rs.getMetaData();// 获取键名
        int columnCount = md.getColumnCount();// 获取行的数量
        while (rs.next()) {
            Map rowData = new HashMap();// 声明Map
            for (int i = 1; i <= columnCount; i++) {
                rowData.put(md.getColumnName(i), rs.getObject(i));// 获取键名及值
                // System.out.println(md.getColumnName(i) + " : " + rs.getObject(i));
            }
            // System.out.println("----------------------------------------------------------------------");
            // list.add(rowData);

            // 将获取的键名getColumnName及值getObject按键名顺序放入list中
            List<Map.Entry<String, Object>> entryList = new ArrayList<>(rowData.entrySet());
            // 通过比较器实现比较排序
            entryList.sort(Map.Entry.comparingByKey());
            list.add(entryList);

        }
        return list;
    }

}
