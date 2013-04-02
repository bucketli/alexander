package com.taobao.alexander.sequence.impl;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.sql.DataSource;

import com.alibaba.druid.pool.DruidDataSource;
import com.taobao.alexander.sequence.Configure;
import com.taobao.alexander.sequence.Configure.DsConfig;
import com.taobao.alexander.sequence.SRange;
import com.taobao.alexander.sequence.Sequence;

/**
 * @description
 * @author <a href="junyu@taobao.com">junyu</a>
 * @date 2013-3-7ÏÂÎç03:26:28
 */
public class ClustedSequenceService {
	public Map<String/* cluster */, Map<String/* slice */, Sequence>> seqs = new HashMap<String, Map<String, Sequence>>();
	public Map<String/* slice */, Map<String/* user */, String/* passwd */>> auths = new HashMap<String, Map<String, String>>();
	private String defaultSlice = "slice1";
	private String defaultCluster = "cluster1";
	private String specifyConf = null;

	public static final String DBURL_FORMAT = "jdbc:mysql://{0}:{1}/{2}";
	public static final String DRIVER = "com.mysql.jdbc.Driver";

	public void init() {
		Configure conf = new Configure();
		if (this.specifyConf != null) {
			conf.setConfigName(specifyConf);
		}
		conf.load();

		auths = conf.getAuth();
		Map<String, Collection<String>> slices = conf.getSlice().asMap();
		for (Map.Entry<String, Collection<String>> s : slices.entrySet()) {
			Map<String, Sequence> smap = seqs.get(s.getKey());
			if (smap == null) {
				smap = new HashMap<String, Sequence>();
				seqs.put(s.getKey(), smap);
			}

			Set<String> sls = (Set<String>) s.getValue();
			for (String sl : sls) {
				RetryableRangeFetcher fetcher = new RetryableRangeFetcher();
				List<DataSource> dslist = new ArrayList<DataSource>();
				Set<DsConfig> dss = conf.getDsConfig().get(s.getKey());
				for (DsConfig ds : dss) {
					MessageFormat f = new MessageFormat(DBURL_FORMAT);
					String url = f.format(new Object[] { ds.ip, ds.port,
							ds.dbName });
					DruidDataSource d = new DruidDataSource();
					d.setDriverClassName(DRIVER);
					d.setUrl(url);
					d.setPoolPreparedStatements(true);
					d.setUsername(ds.user);
					d.setPassword(ds.passwd);
					dslist.add(d);
				}
				fetcher.setDataSources(dslist);

				RetryableSequence sequence = new RetryableSequence();
				sequence.setFetcher(fetcher);
				sequence.setSlice(sl);
				sequence.init();
				smap.put(sl, sequence);
			}
		}
	}

	public long nextVal(String cluster, String slice) {
		Sequence s = getSequence(cluster, slice);
		return s.nextValue();
	}

	public SRange nextRange(String cluster, String slice) {
		Sequence s = getSequence(cluster, slice);
		return s.nextRange();
	}

	private Sequence getSequence(String cluster, String slice) {
		if (cluster == null) {
			cluster = defaultCluster;
		}

		Map<String, Sequence> c = seqs.get(cluster);
		if (c == null) {
			throw new RuntimeException("sequence cluster " + cluster
					+ " not exist");
		}

		if (slice == null) {
			slice = this.defaultSlice;
		}
		Sequence s = c.get(slice);
		if (s == null) {
			throw new RuntimeException("sequence cluster [" + cluster
					+ "] has no [" + slice + "] slice");
		}
		
		return s;
	}

	public String getDefaultSlice() {
		return defaultSlice;
	}

	public String getDefaultCluster() {
		return defaultCluster;
	}

	public String getSpecifyConf() {
		return specifyConf;
	}

	public void setSpecifyConf(String specifyConf) {
		this.specifyConf = specifyConf;
	}

	public boolean checkUserAndPasswd(String user, String passwd) {
		for (Map.Entry<String, Map<String, String>> up : auths.entrySet()) {
			String p = up.getValue().get(user);
			if (p != null) {
				if (!p.equals(passwd)) {
					return false;
				} else {
					return true;
				}
			}
		}

		return false;
	}

	public String getPasswd(String user) {
		for (Map.Entry<String, Map<String, String>> up : auths.entrySet()) {
			String p = up.getValue().get(user);
			if (p != null) {
				return p;
			}
		}

		return null;
	}

	public Map<String, Map<String, String>> getAuths() {
		return auths;
	}
}
