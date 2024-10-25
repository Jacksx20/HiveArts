package com.huawei.bigdata.hive;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class WriteDb {

	// 初始化参数
	private static void init() throws IOException {
		System.out.println("初始化配...........");

		String KRB5_FILE = "src/main/resources/krb/krb5.conf";
		String ZOOKEEPER_DEFAULT_SERVER_PRINCIPAL = "zookeeper/hadoop.hadoop.com";
		String ZOOKEEPER_SERVER_PRINCIPAL_KEY = "zookeeper.server.principal";

		// 设置Kerberos认证的凭据
		System.setProperty("java.security.krb5.conf", KRB5_FILE);
		// 设置Zookeeper服务的主体
		System.setProperty(ZOOKEEPER_SERVER_PRINCIPAL_KEY, ZOOKEEPER_DEFAULT_SERVER_PRINCIPAL);
	}

	public static void main(String[] args) throws IOException {
		init();
		Connection connection = null;
		// 使用你的数据库URL、用户名和密码替换以下字符串
		String url = "jdbc:hive2://10.97.213.6:24002,10.97.213.5:24002,10.97.213.4:24002/;serviceDiscoveryMode=zooKeeper;mapreduce.job.queuename=bigdata_prd;zooKeeperNamespace=hiveserver2;sasl.qop=auth-conf;auth=KERBEROS;principal=hive/hadoop.hadoop.com@HADOOP.COM;user.principal=W0008817;user.keytab=src/main/resources/krb/user.keytab;";

		try {
			Class.forName("org.apache.hive.jdbc.HiveDriver"); // 加载并注册JDBC驱动
			connection = DriverManager.getConnection(url, "", "");
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
			rs.close();

			// 关闭结果集、声明和连接

			stmt.close();
			connection.close();

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	private static List getList(ResultSet rs) throws SQLException {
		List list = new ArrayList();

		ResultSetMetaData md = rs.getMetaData();// 获取键名
		int columnCount = md.getColumnCount();// 获取行的数量
		while (rs.next()) {
			Map rowData = new HashMap();// 声明Map
			for (int i = 1; i <= columnCount; i++) {
				rowData.put(md.getColumnName(i), rs.getObject(i));// 获取键名及值
			}
			list.add(rowData);
		}
		return list;
	}

}
