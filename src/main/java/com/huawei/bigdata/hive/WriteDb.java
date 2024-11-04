package com.huawei.bigdata.hive;

import java.sql.Connection;
import java.sql.DriverManager;

public class WriteDb {

	public static void main(String[] args) {
		System.out.println("初始化配置...........");

		// 设置Kerberos认证的凭据
		System.setProperty("java.security.krb5.conf", "src/main/resources/Kerberos/krb5.conf");
		// 设置Zookeeper服务的主体
		System.setProperty("zookeeper.server.principal", "zookeeper/hadoop.hadoop.com");

		Connection connection = null;
		String url = "jdbc:hive2://10.97.213.6:24002,10.97.213.5:24002,10.97.213.4:24002/;serviceDiscoveryMode=zooKeeper;mapreduce.job.queuename=bigdata_prd;zooKeeperNamespace=hiveserver2;sasl.qop=auth-conf;auth=KERBEROS;principal=hive/hadoop.hadoop.com@HADOOP.COM;user.principal=W0008818;user.keytab=src/main/resources/Kerberos/user.keytab;";

		try {
			Class.forName("org.apache.hive.jdbc.HiveDriver"); // 加载并注册JDBC驱动
			connection = DriverManager.getConnection(url, "", "");
			System.out.println("连接成功...........");
			connection.close();
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("连接失败...........");
		}

	}

}
