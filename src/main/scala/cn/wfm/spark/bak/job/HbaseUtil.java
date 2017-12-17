package cn.wfm.spark.bak.job;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

/**
 * 获取数据库的连接工具
 * @author lyd
 *
 */
public class HbaseUtil {
	public static final Logger logger = Logger.getLogger(HbaseUtil.class);
	public static final String NAME = "hbase.zookeeper.quorum";
	public static final String VALUE = "hadoop1:2181,hadoop2:2181,hadoop3:2181";
	static Configuration conf = null;
	static Connection conn = null;
	static {
		conf = HBaseConfiguration.create();
		conf.set(NAME, VALUE);
		try {
			conn = ConnectionFactory.createConnection(conf);
		} catch (IOException e) {
			logger.error("获取connection有异常", e);
		}
	}
	/**
	 * 获取hbaseAdmin的工具
	 * @return
	 */
	public static Admin getAdmin(){
		Admin admin = null;
		try {
			admin = conn.getAdmin();
		} catch (Exception e) {
			logger.error("获取connection有异常", e);
		}
		return admin;
	}
	
	/**
	 * 关闭Admin
	 * @param admin
	 */
	public static void closeAdmin(Admin admin){
		if(admin != null){
			try {
				admin.close();
			} catch (IOException e) {
				//do nothing
			}
		}
	}
	
	
	/**
	 * 获取表
	 * @return
	 */
	public static Table getTable(String tableName){
		Table table = null;
		try {
			//HTable ht = new HTable(conf, tableName); //过时的
			table = conn.getTable(TableName.valueOf(tableName));
		} catch (IOException e) {
			logger.error("获取表异常", e);
		}
		return table;
	}
	
	/**
	 * 关闭table
	 * @param table
	 */
	public static void closeTable(Table table){
		if(table != null){
			try {
				table.close();
			} catch (IOException e) {
				//do nothing
			}
		}
	}
	
	/**
	 * 打印
	 * @param rs
	 */
	public static void print(Result rs){
			 NavigableMap<byte[], byte[]> map =  rs.getFamilyMap("info".getBytes());
			 for (Entry<byte[], byte[]> result : map.entrySet()) {
				System.out.print(Bytes.toString(result.getKey())+":"+Bytes.toString(result.getValue())+"\t");
			 }
				System.out.println();
	}
	
	/**
	 * 
	 * @param rs
	 */
	public static void print(ResultScanner rs){
		for (Result r: rs) {
			 print(r);
		 }
	}

	public static void dataToHabse(String tbName,String rowKey,String famliy,String qualifier,String data){
		Table table = getTable(tbName);
		Put put = new Put(Bytes.toBytes(rowKey));
		put.addColumn(Bytes.toBytes(famliy),Bytes.toBytes(qualifier),Bytes.toBytes(data));
		try {
			table.put(put);
		} catch (IOException e) {
			e.printStackTrace();
		}
		closeTable(table);
	}
	public static void dataToHabse(String tbName, String rowKey, String famliy, HashMap<String,String> data){
		Table table = getTable(tbName);
		Set<Entry<String, String>> entries = data.entrySet();
		List<Put> list = new ArrayList<>();
		for(Entry<String, String> entry : entries){
			Put put = new Put(Bytes.toBytes(rowKey));
			put.addColumn(Bytes.toBytes(famliy),Bytes.toBytes(entry.getKey()),Bytes.toBytes(entry.getValue()));
			list.add(put);
		}

		try {
			table.put(list);
		} catch (IOException e) {
			e.printStackTrace();
		}
		closeTable(table);
	}

}
