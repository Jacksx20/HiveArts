
package com.huawei.bigdata.hive;

import com.huawei.bigdata.security.KerberosUtil;
import com.huawei.bigdata.security.LoginUtil;
import org.apache.hadoop.conf.Configuration;
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
public class JDBCClient {
    private static final Logger logger = LoggerFactory.getLogger(JDBCClient.class);
    private static final String HIVE_DRIVER = "org.apache.hive.jdbc.HiveDriver";

    @SuppressWarnings("unused")
    private static final String ZOOKEEPER_DEFAULT_LOGIN_CONTEXT_NAME = "Client";
    private static final String ZOOKEEPER_SERVER_PRINCIPAL_KEY = "zookeeper.server.principal";
    private static String ZOOKEEPER_DEFAULT_SERVER_PRINCIPAL = null;

    @SuppressWarnings("unused")
    private static Configuration CONF = null;
    private static String KRB5_FILE = null;
    private static String USER_NAME = null;
    private static String USER_KEYTAB_FILE = null;

    /** yarn 队列名称 */
    private static String JOB_QUEUE_NAME = null;

    /* zookeeper节点ip和端口列表 */
    private static String zkQuorum = null;
    private static String auth = null;
    private static String sasl_qop = null;
    private static String zooKeeperNamespace = null;
    private static String serviceDiscoveryMode = null;
    private static String principal = null;
    private static String AUTH_HOST_NAME = null;

    /**
     * Get user realm process
     */
    public static String getUserRealm() {
        String serverRealm = System.getProperty("SERVER_REALM");
        if (serverRealm != null && serverRealm != "") {
            AUTH_HOST_NAME = "hadoop." + serverRealm.toLowerCase();
        } else {
            serverRealm = KerberosUtil.getKrb5DomainRealm();
            if (serverRealm != null && serverRealm != "") {
                AUTH_HOST_NAME = "hadoop." + serverRealm.toLowerCase();
            } else {
                AUTH_HOST_NAME = "hadoop";
            }
        }
        return AUTH_HOST_NAME;
    }

    // 初始化参数
    private static void init() throws IOException {
        System.out.println("初始化配置文件...........");
        CONF = new Configuration();
        // 客户端配置文件
        Properties clientInfo = null;
        // 寻找当前用户目录下的配置文件
        ClassLoader resourceLoader = JDBCClient.class.getClassLoader();
        String userdir = System.getProperty("user.dir")
                + File.separator
                + "src"
                + File.separator
                + "main"
                + File.separator
                + "resources"
                + File.separator;
        InputStream fileInputStream = null;
        try {
            clientInfo = new Properties();
            /**
             * "hiveclient.properties"为客户端配置文件，如果使用多实例特性，需要把该文件换成对应实例客户端下的"hiveclient.properties"
             * "hiveclient.properties"文件位置在对应实例客户端安裝包解压目录下的config目录下
             */
            // String hiveclientProp = userdir + "hiveclient.properties";
            String hiveclientProp = resourceLoader.getResource("hiveclient.properties").getPath();
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
        /**
         * zkQuorum获取后的格式为"xxx.xxx.xxx.xxx:24002,xxx.xxx.xxx.xxx:24002,xxx.xxx.xxx.xxx:24002";
         * "xxx.xxx.xxx.xxx"为集群中ZooKeeper所在节点的业务IP，端口默认是24002
         */
        zkQuorum = clientInfo.getProperty("zk.quorum");
        auth = clientInfo.getProperty("auth");
        sasl_qop = clientInfo.getProperty("sasl.qop");
        zooKeeperNamespace = clientInfo.getProperty("zooKeeperNamespace");
        serviceDiscoveryMode = clientInfo.getProperty("serviceDiscoveryMode");
        principal = clientInfo.getProperty("principal");
        @SuppressWarnings("unused")
        String krb5Path = resourceLoader.getResource("krb5.conf").getPath();
        KRB5_FILE = userdir + "krb5.conf";
        System.setProperty("java.security.krb5.conf", KRB5_FILE);
        // 设置新建用户的USER_NAME，其中"xxx"指代之前创建的用户名，例如创建的用户为user，则USER_NAME为user
        USER_NAME = "W0008817";
        JOB_QUEUE_NAME = clientInfo.getProperty("mapreduce.job.queuename");

        if ("KERBEROS".equalsIgnoreCase(auth)) {
            // 设置客户端的keytab和zookeeper认证principal
            // USER_KEYTAB_FILE = "src/main/resources/user.keytab";

            USER_KEYTAB_FILE = resourceLoader.getResource("user.keytab").getPath();
            ZOOKEEPER_DEFAULT_SERVER_PRINCIPAL = "zookeeper/" + getUserRealm();
            System.setProperty(ZOOKEEPER_SERVER_PRINCIPAL_KEY, ZOOKEEPER_DEFAULT_SERVER_PRINCIPAL);
        }

        // zookeeper开启ssl时需要设置JVM参数
        LoginUtil.processZkSsl(clientInfo);
    }

    public static void insertDeleteData() throws IOException {
        // 参数初始化
        init();

    }

    /**
     * 本示例演示了如何使用Hive JDBC接口来执行HQL命令<br>
     * <br>
     *
     * @throws ClassNotFoundException
     * @throws SQLException
     * @throws IOException
     */
    @SuppressWarnings("rawtypes")
    public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException {

        // 参数初始化
        init();
        // 拼接JDBC URL
        StringBuilder strBuilder = new StringBuilder("jdbc:hive2://").append(zkQuorum).append("/");
        // StringBuilder strBuilder = new
        // StringBuilder("jdbc:hive2://10.97.213.4:21066/bigdata_dev;sasl.qop=auth-conf;auth=KERBEROS;principal=hive/hadoop.hadoop.com@HADOOP.COM").append(zkQuorum).append("/");

        if ("KERBEROS".equalsIgnoreCase(auth)) {
            strBuilder
                    .append(";serviceDiscoveryMode=")
                    .append(serviceDiscoveryMode)
                    .append(";mapreduce.job.queuename=")
                    .append(JOB_QUEUE_NAME)
                    .append(";zooKeeperNamespace=")
                    .append(zooKeeperNamespace)
                    .append(";sasl.qop=")
                    .append(sasl_qop)
                    .append(";auth=")
                    .append(auth)
                    .append(";principal=")
                    .append(principal)
                    .append(";user.principal=")
                    .append(USER_NAME)
                    .append(";user.keytab=")
                    .append(USER_KEYTAB_FILE)
                    .append(";");
        } else {
            /* 普通模式 */
            strBuilder
                    .append(";serviceDiscoveryMode=")
                    .append(serviceDiscoveryMode)
                    .append(";zooKeeperNamespace=")
                    .append(zooKeeperNamespace)
                    .append(";auth=none");
        }
        String url = strBuilder.toString();
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
