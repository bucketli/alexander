package com.taobao.alexander.sequence;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import com.google.common.collect.HashMultimap;

/**
 * @description
 * @author <a href="junyu@taobao.com">junyu</a>
 * @date 2013-3-19ÏÂÎç01:15:50
 */
public class Configure {
	private Set<String> cluster = new HashSet<String>();
	private HashMultimap<String, DsConfig> dsConfig = HashMultimap
			.<String, DsConfig> create();
	private HashMultimap<String, String> slice = HashMultimap
			.<String, String> create();
	private Map<String, Map<String, String>> auth = new HashMap<String, Map<String, String>>();
	private String configName = "sequence.properties";

	public void load() {
		if (configName != null && !"".equals(configName.trim())) {
			InputStream is = Configure.class.getResourceAsStream(configName);
			if (is == null) {
				is = ClassLoader.getSystemResourceAsStream(configName);
			}

			if (is == null) {
				throw new RuntimeException(
						"no config at all,the config file in classpath is:"
								+ configName);
			}

			Properties prop = new Properties();
			try {
				prop.load(is);
			} catch (IOException e) {
				throw new RuntimeException(e);
			} finally{
				try {
					is.close();
				} catch (IOException e) {
					//ignore
				}
			}

			String clusters = prop.getProperty("clusters");
			if (clusters != null && !"".equals(clusters.trim())) {
				String[] cs = clusters.trim().split(",");
				for (String c : cs) {
					if (c != null && !"".equals(c.trim())) {
						cluster.add(c);
						String slices = getSlice(c, prop);
						if (slices != null && !"".equals(slices.trim())) {
							String[] ss = slices.trim().split(",");
							for (String s : ss) {
								if (s != null && !"".equals(s.trim())) {
									slice.put(c, s);
									Map<String, String> m = auth.get(s);
									if (m == null) {
										m = new HashMap<String, String>();
										auth.put(s, m);
									}

									String users = getSliceUsers(c, s, prop);
									if (users != null && !"".equals(s.trim())) {
										String[] ups = users.split(",");
										for (String up : ups) {
											if (up != null
													&& !"".equals(up.trim())) {
												String[] uandp = up.split("\\|");
												m.put(uandp[0], uandp[1]);
											}
										}
									}
								}
							}
						}

						String dbs = getDbs(c, prop);
						if (dbs != null && !"".equals(dbs.trim())) {
							String[] ds = dbs.split(",");
							for (String d : ds) {
								if (d != null && !"".equals(d.trim())) {
									String ip = getIp(c, d, prop);
									String port = getPort(c, d, prop);
									String user = getUser(c, d, prop);
									String passwd = getPasswd(c, d, prop);
									String dbName = getDbName(c, d, prop);
									DsConfig dc = new DsConfig(ip, port,
											dbName, user, passwd);
									dsConfig.put(c, dc);
								}
							}
						}
					}
				}
			}
		} else {
			throw new RuntimeException("configName is null or empty!");
		}
	}

	public static String getSlice(String cluster, Properties prop) {
		StringBuilder sb = new StringBuilder();
		sb.append(cluster);
		sb.append(".slices");
		return prop.getProperty(sb.toString());
	}

	public static String getSliceUsers(String cluster, String slice,
			Properties prop) {
		StringBuilder sb = new StringBuilder();
		sb.append(cluster);
		sb.append(".");
		sb.append(slice);
		sb.append(".user");
		return prop.getProperty(sb.toString());
	}

	public static String getDbs(String cluster, Properties prop) {
		StringBuilder sb = new StringBuilder();
		sb.append(cluster);
		sb.append(".dbs");
		return prop.getProperty(sb.toString());
	}

	public static String getIp(String cluster, String db, Properties prop) {
		StringBuilder sb = new StringBuilder();
		sb.append(cluster);
		sb.append(".");
		sb.append(db);
		sb.append(".ip");
		return prop.getProperty(sb.toString());
	}

	public static String getPort(String cluster, String db, Properties prop) {
		StringBuilder sb = new StringBuilder();
		sb.append(cluster);
		sb.append(".");
		sb.append(db);
		sb.append(".port");
		return prop.getProperty(sb.toString());
	}

	public static String getUser(String cluster, String db, Properties prop) {
		StringBuilder sb = new StringBuilder();
		sb.append(cluster);
		sb.append(".");
		sb.append(db);
		sb.append(".user");
		return prop.getProperty(sb.toString());
	}

	public static String getPasswd(String cluster, String db, Properties prop) {
		StringBuilder sb = new StringBuilder();
		sb.append(cluster);
		sb.append(".");
		sb.append(db);
		sb.append(".passwd");
		return prop.getProperty(sb.toString());
	}

	public static String getDbName(String cluster, String db, Properties prop) {
		StringBuilder sb = new StringBuilder();
		sb.append(cluster);
		sb.append(".");
		sb.append(db);
		sb.append(".dbName");
		return prop.getProperty(sb.toString());
	}

	public Set<String> getCluster() {
		return cluster;
	}

	public void setCluster(Set<String> cluster) {
		this.cluster = cluster;
	}

	public HashMultimap<String, DsConfig> getDsConfig() {
		return dsConfig;
	}

	public void setDsConfig(HashMultimap<String, DsConfig> dsConfig) {
		this.dsConfig = dsConfig;
	}

	public HashMultimap<String, String> getSlice() {
		return slice;
	}

	public void setSlice(HashMultimap<String, String> slice) {
		this.slice = slice;
	}

	public String getConfigName() {
		return configName;
	}

	public void setConfigName(String configName) {
		this.configName = configName;
	}

	public Map<String, Map<String, String>> getAuth() {
		return auth;
	}

	public void setAuth(Map<String, Map<String, String>> auth) {
		this.auth = auth;
	}

	public class DsConfig {
		public final String ip;
		public final String port;
		public final String dbName;
		public final String user;
		public final String passwd;

		public DsConfig(String ip, String port, String dbName, String user,
				String passwd) {
			this.ip = ip;
			this.port = port;
			this.dbName = dbName;
			this.user = user;
			this.passwd = passwd;
		}
	}
}
