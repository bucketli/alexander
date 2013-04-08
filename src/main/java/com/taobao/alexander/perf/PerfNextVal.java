package com.taobao.alexander.perf;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.alibaba.druid.pool.DruidDataSource;
import com.taobao.alexander.sequence.SequenceServer;
import com.taobao.alexander.sequence.impl.ClustedSequenceService;

/**
 * @description
 * @author <a href="junyu@taobao.com">junyu</a>
 * @date 2013-4-8ÏÂÎç02:13:27
 */
public class PerfNextVal {
	public static final Log log = LogFactory.getLog(PerfNextVal.class);
	public static final String PERF_CONFIG_FILE_NAME="perf.properties";

	private String ip="127.0.0.1";
	private String port="8507";
	private String dbName="test";
	private String userName="junyu";
	private String passwd="123";
	private int threadCount=10;
	
	/**
	 * @param args
	 * @throws SQLException
	 * @throws IOException 
	 */
	public static void main(String[] args) throws SQLException, IOException {
		PerfNextVal v = new PerfNextVal();
		v.init();
		v.test();
	}

	private DruidDataSource ds = null;

	public void init() throws IOException {
		InputStream is=SequenceServer.class.getClassLoader().getResourceAsStream(PERF_CONFIG_FILE_NAME);
		if(is==null){
			is=ClassLoader.getSystemResourceAsStream(PERF_CONFIG_FILE_NAME);
		}
		
		if(is!=null){
			Properties prop=new Properties();
			try {
				prop.load(is);
			} catch (IOException e) {
				throw e;
			} finally{
				is.close();
			}
			
			String sIp=prop.getProperty("dst_ip");
			String sPort=prop.getProperty("dst_port");
			String sDbname=prop.getProperty("dst_dbname");
			String sUsername=prop.getProperty("dst_username");
			String sPasswd=prop.getProperty("dst_passwd");
			String sThread=prop.getProperty("local_thread");
			
			if(sIp!=null&&!"".equals(sIp.trim())){
				this.ip=sIp;
			}
			
			if(sPort!=null&&!"".equals(sPort.trim())){
				this.port=sPort;
			}
			
			if(sDbname!=null&&!"".equals(sDbname.trim())){
				this.dbName=sDbname;
			}
			
			if(sUsername!=null&&!"".equals(sUsername.trim())){
				this.userName=sUsername;
			}
			
			if(sPasswd!=null&&!"".equals(sPasswd.trim())){
				this.passwd=sPasswd;
			}
			
			if(sThread!=null&&!"".equals(sThread.trim())){
				this.threadCount=Integer.valueOf(threadCount);
			}
		}
	
		MessageFormat f = new MessageFormat(ClustedSequenceService.DBURL_FORMAT);
		String url = f.format(new Object[] { ip, port, dbName });
		ds = new DruidDataSource();
		ds.setDriverClassName(ClustedSequenceService.DRIVER);
		ds.setUrl(url);
		ds.setPoolPreparedStatements(true);
		ds.setUsername(userName);
		ds.setPassword(passwd);
		ds.setMaxActive(threadCount+2);
	}

	public AtomicLong al = new AtomicLong(0);

	public void test() throws SQLException {
		ExecutorService es = Executors.newFixedThreadPool(threadCount+1);
		for (int i = 0; i < threadCount; i++) {
			es.execute(new r());
		}

		long last = 0;
		int intevalmill = 1000;
		while (true) {
			try {
				Thread.sleep(intevalmill);
			} catch (InterruptedException e) {
				log.error(e);
			}
			long now = al.get();
			log.info("qps:" + ((now - last) * 1000) / intevalmill);
			last = now;
		}
	}

	public class r implements Runnable {
		@Override
		public void run() {
			Connection c=null;
			try {
				c = ds.getConnection();
				while (true) {
					PreparedStatement ps = c
							.prepareStatement("select next_val()");
					ResultSet rs = ps.executeQuery();
					while (rs.next()) {
						rs.getLong(3);
					}
					
					rs.close();
					ps.close();
					al.incrementAndGet();
				}
			} catch (SQLException e) {
				log.error(e);
			} finally {
				if (c != null) {
					try {
						c.close();
					} catch (SQLException e) {
						log.error(e);
					}
				}
			}
		}
	}
}
