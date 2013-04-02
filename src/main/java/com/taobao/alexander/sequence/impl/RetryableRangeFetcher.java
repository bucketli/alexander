package com.taobao.alexander.sequence.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import javax.sql.DataSource;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.alexander.sequence.RangeFetcher;
import com.taobao.alexander.sequence.SRange;

/**
 * @description
 * @author <a href="junyu@taobao.com">junyu</a>
 * @date 2013-3-7下午02:30:42
 */
public class RetryableRangeFetcher implements RangeFetcher {
	private static final Log log = LogFactory
			.getLog(RetryableRangeFetcher.class);

	private static final int DEFAULT_INNER_STEP = 1000;
	private static final int DEFAULT_RETRY_TIMES = 2;
	private static final String DEFAULT_TABLE_NAME = "sequence";
	private static final String DEFAULT_NAME_COLUMN_NAME = "name";
	private static final String DEFAULT_VALUE_COLUMN_NAME = "value";
	private static final String DEFAULT_GMT_MODIFIED_COLUMN_NAME = "gmt_modified";

	private static final boolean DEFAULT_ADJUST = true;

	private static final long DELTA = 100000000L;

	/**
	 * 数据源
	 */
	private List<DataSource> dataSources;

	/**
	 * 自适应开关
	 */
	private boolean adjust = DEFAULT_ADJUST;
	/**
	 * 重试次数
	 */
	private int retryTimes = DEFAULT_RETRY_TIMES;

	/**
	 * 内步长
	 */
	private int innerStep = DEFAULT_INNER_STEP;

	/**
	 * 外步长
	 */
	private int outStep = DEFAULT_INNER_STEP;

	/**
	 * 序列所在的表名
	 */
	private String tableName = DEFAULT_TABLE_NAME;

	/**
	 * 存储序列名称的列名
	 */
	private String nameColumnName = DEFAULT_NAME_COLUMN_NAME;

	/**
	 * 存储序列值的列名
	 */
	private String valueColumnName = DEFAULT_VALUE_COLUMN_NAME;

	/**
	 * 存储序列最后更新时间的列名
	 */
	private String gmtModifiedColumnName = DEFAULT_GMT_MODIFIED_COLUMN_NAME;

	private volatile String selectSql;
	private volatile String updateSql;
	private volatile String insertSql;

	/**
	 * 初试化
	 * 
	 * @throws SequenceException
	 */
	public void init() {
		if (dataSources == null) {
			log.error("生成sequence的datasource map为空");
			throw new RuntimeException("生成sequence的datasource map为空");
		}

		outStep = innerStep * (dataSources.size());// 计算外步长
		StringBuilder sb = new StringBuilder();
		sb.append("GroupSequenceDao初始化完成：\r\n ");
		sb.append("innerStep:").append(this.innerStep).append("\r\n");
		sb.append("dataSource size:").append(dataSources.size()).append("个:");
		sb.append("\r\n");
		sb.append("adjust：").append(adjust).append("\r\n");
		sb.append("retryTimes:").append(retryTimes).append("\r\n");
		sb.append("tableName:").append(tableName).append("\r\n");
		sb.append("nameColumnName:").append(nameColumnName).append("\r\n");
		sb.append("valueColumnName:").append(valueColumnName).append("\r\n");
		sb.append("gmtModifiedColumnName:").append(gmtModifiedColumnName)
				.append("\r\n");
		log.info(sb.toString());
	}

	/**
	 * 
	 * @param index
	 *            gourp内的序号，从0开始
	 * @param value
	 *            当前取的值
	 * @return
	 */
	private boolean check(int index, long value) {
		return (value % outStep) == (index * innerStep);
	}

	/**
	 * 检查并初试某个sequence 1、如果sequece不处在，插入值，并初始化值 2、如果已经存在，但有重叠，重新生成
	 * 3、如果已经存在，且无重叠。
	 * 
	 * @throws SequenceException
	 */
	public void adjust(String slice) {
		Connection conn = null;
		PreparedStatement stmt = null;
		ResultSet rs = null;

		for (int i = 0; i < dataSources.size(); i++) {
			DataSource ds = dataSources.get(i);
			try {
				conn = ds.getConnection();
				stmt = conn.prepareStatement(getSelectSql());
				stmt.setString(1, slice);
				rs = stmt.executeQuery();
				int item = 0;
				while (rs.next()) {
					item++;
					long val = rs.getLong(this.getValueColumnName());
					if (!check(i, val)) // 检验初值
					{
						if (this.isAdjust()) {
							this.adjustUpdate(i, val, slice);
						} else {
							log.error("数据库中配置的初值出错！请调整你的数据库，或者启动adjust开关");
							throw new RuntimeException(
									"数据库中配置的初值出错！请调整你的数据库，或者启动adjust开关");
						}
					}
				}
				if (item == 0)// 不存在,插入这条记录
				{
					if (this.isAdjust()) {
						this.adjustInsert(i, slice);
					} else {
						log.error("数据库中未配置该sequence！请往数据库中插入sequence记录，或者启动adjust开关");
						throw new RuntimeException(
								"数据库中未配置该sequence！请往数据库中插入sequence记录，或者启动adjust开关");
					}
				}
			} catch (SQLException e) {// 吞掉SQL异常，我们允许不可用的库存在
				log.error("初值校验和自适应过程中出错.", e);
				throw new RuntimeException(e);
			} finally {
				closeResultSet(rs);
				rs = null;
				closeStatement(stmt);
				stmt = null;
				closeConnection(conn);
				conn = null;

			}

		}
	}

	/**
	 * 更新
	 * 
	 * @param index
	 * @param value
	 * @param slice
	 * @throws SequenceException
	 * @throws SQLException
	 */
	private void adjustUpdate(int index, long value, String slice)
			throws SQLException {
		long newValue = (value - value % outStep) + outStep + index * innerStep;// 设置成新的调整值
		DataSource ds = this.dataSources.get(index);
		Connection conn = null;
		PreparedStatement stmt = null;
		try {
			conn = ds.getConnection();
			stmt = conn.prepareStatement(getUpdateSql());
			stmt.setLong(1, newValue);
			stmt.setTimestamp(2, new Timestamp(System.currentTimeMillis()));
			stmt.setString(3, slice);
			stmt.setLong(4, value);
			int affectedRows = stmt.executeUpdate();
			if (affectedRows == 0) {
				throw new RuntimeException(
						"faild to auto adjust init value at  " + slice
								+ " update affectedRow =0");
			}
			log.info("datasource " + index + " 更新初值成功!" + "sequence Name："
					+ slice + "更新过程：" + value + "-->" + newValue);
		} catch (SQLException e) { // 吃掉SQL异常，抛Sequence异常
			log.error("由于SQLException,更新初值自适应失败！dbGroupIndex:datasource "
					+ index + ",sequence Name：" + slice + "更新过程：" + value
					+ "-->" + newValue, e);
			throw new RuntimeException(
					"由于SQLException,更新初值自适应失败！dbGroupIndex:+datasource+"
							+ index + ",sequence Name：" + slice + "更新过程："
							+ value + "-->" + newValue, e);
		} finally {
			closeStatement(stmt);
			stmt = null;
			closeConnection(conn);
			conn = null;
		}
	}

	/**
	 * 插入新值
	 * 
	 * @param index
	 * @param name
	 * @return
	 * @throws SequenceException
	 * @throws SQLException
	 */
	private void adjustInsert(int index, String slice) throws SQLException {
		DataSource ds = this.dataSources.get(index);
		long newValue = index * innerStep;
		Connection conn = null;
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try {
			conn = ds.getConnection();
			stmt = conn.prepareStatement(getInsertSql());
			stmt.setString(1, slice);
			stmt.setLong(2, newValue);
			stmt.setTimestamp(3, new Timestamp(System.currentTimeMillis()));
			int affectedRows = stmt.executeUpdate();
			if (affectedRows == 0) {
				throw new RuntimeException(
						"faild to auto adjust init value at  " + slice
								+ " update affectedRow =0");
			}
			log.info("datasource " + index + "   name:" + slice + "插入初值:"
					+ slice + "value:" + newValue);

		} catch (SQLException e) { // 吃掉SQL异常，抛sequence异常
			log.error("由于SQLException,插入初值自适应失败！dbGroupIndex:datasource "
					+ index + ",sequence Name：" + slice + "   value:"
					+ newValue, e);
			throw new RuntimeException(
					"由于SQLException,插入初值自适应失败！dbGroupIndex:datasource " + index
							+ "sequence Name：" + slice + "   value:" + newValue,
					e);
		} finally {
			closeResultSet(rs);
			rs = null;
			closeStatement(stmt);
			stmt = null;
			closeConnection(conn);
			conn = null;
		}
	}

	private ConcurrentHashMap<Integer/* ds index */, AtomicInteger/* 掠过次数 */> excludedKeyCount = new ConcurrentHashMap<Integer, AtomicInteger>();
	// 最大略过次数后恢复
	private int maxSkipCount = 10;
	// 使用慢速数据库保护
	private boolean useSlowProtect = false;
	// 保护的时间
	private int protectMilliseconds = 50;

	private ExecutorService exec = Executors.newFixedThreadPool(1);

	public SRange nextRange(final String slice) throws RuntimeException {
		if (slice == null) {
			log.error("序列名为空！");
			throw new IllegalArgumentException("序列名称不能为空");
		}

		long oldValue;
		long newValue;

		boolean readSuccess;
		boolean writeSuccess;

		Connection conn = null;
		PreparedStatement stmt = null;
		ResultSet rs = null;

		int[] randomIntSequence = RandomSequence.randomIntSequence(dataSources
				.size());
		for (int i = 0; i < retryTimes; i++) {
			for (int j = 0; j < this.dataSources.size(); j++) {
				readSuccess = false;
				writeSuccess = false;
				int index = randomIntSequence[j];
				if (excludedKeyCount.get(index) != null) {
					if (excludedKeyCount.get(index).incrementAndGet() > maxSkipCount) {
						excludedKeyCount.remove(index);
						log.error(maxSkipCount + "次数已过，index为" + index
								+ "的数据源后续重新尝试取序列");
					} else {
						continue;
					}
				}

				final DataSource ds = dataSources.get(index);
				// 查询，只在这里做数据库挂掉保护和慢速数据库保护
				try {
					// 如果未使用超时保护或者已经只剩下了1个数据源，无论怎么样去拿
					if (!useSlowProtect
							|| excludedKeyCount.size() >= (dataSources.size() - 1)) {
						conn = ds.getConnection();
						stmt = conn.prepareStatement(getSelectSql());
						stmt.setString(1, slice);
						rs = stmt.executeQuery();
						rs.next();
						oldValue = rs.getLong(1);
					} else {
						FutureTask<Long> future = new FutureTask<Long>(
								new Callable<Long>() {
									public Long call() throws Exception {
										// 直接抛出异常外面接，但是这里需要直接关闭链接
										Connection fconn = null;
										PreparedStatement fstmt = null;
										ResultSet frs = null;
										try {
											fconn = ds.getConnection();
											fstmt = fconn
													.prepareStatement(getSelectSql());
											fstmt.setString(1, slice);
											frs = fstmt.executeQuery();
											frs.next();
											return frs.getLong(1);
										} finally {
											closeResultSet(frs);
											frs = null;
											closeStatement(fstmt);
											fstmt = null;
											closeConnection(fconn);
											fconn = null;
										}
									}
								});

						try {
							exec.submit(future);
							oldValue = future.get(protectMilliseconds,
									TimeUnit.MILLISECONDS);
						} catch (InterruptedException e) {
							throw new SQLException(
									"[SEQUENCE SLOW-PROTECTED MODE]:InterruptedException",
									e);
						} catch (ExecutionException e) {
							throw new SQLException(
									"[SEQUENCE SLOW-PROTECTED MODE]:ExecutionException",
									e);
						} catch (TimeoutException e) {
							throw new SQLException(
									"[SEQUENCE SLOW-PROTECTED MODE]:TimeoutException,当前设置超时时间为"
											+ protectMilliseconds, e);
						}
					}

					if (oldValue < 0) {
						StringBuilder message = new StringBuilder();
						message.append(
								"Sequence value cannot be less than zero, value = ")
								.append(oldValue);
						message.append(", please check table ").append(
								getTableName());
						log.info(message);

						continue;
					}
					if (oldValue > Long.MAX_VALUE - DELTA) {
						StringBuilder message = new StringBuilder();
						message.append("Sequence value overflow, value = ")
								.append(oldValue);
						message.append(", please check table ").append(
								getTableName());
						log.info(message);
						continue;
					}

					newValue = oldValue + outStep;
					if (!check(index, newValue)) // 新算出来的值有问题
					{
						if (this.isAdjust()) {
							newValue = (newValue - newValue % outStep)
									+ outStep + index * innerStep;// 设置成新的调整值
						} else {
							RuntimeException sequenceException = new RuntimeException(
									"datasource "
											+ index
											+ ":"
											+ slice
											+ "的值得错误，覆盖到其他范围段了！请修改数据库，或者开启adjust开关！");

							log.error("datasource" + index + ":" + slice
									+ "的值得错误，覆盖到其他范围段了！请修改数据库，或者开启adjust开关！",
									sequenceException);
							throw sequenceException;
						}
					}
				} catch (SQLException e) {
					log.error("取范围过程中--查询出错！datasource " + i + ":" + slice, e);
					// 如果数据源只剩下了最后一个，就不要排除了
					if (excludedKeyCount.size() < (dataSources.size() - 1)) {
						excludedKeyCount.put(index, new AtomicInteger(0));
						log.error("暂时踢除index为" + index + "的数据源，" + maxSkipCount
								+ "次后重新尝试");
					}

					continue;
				} finally {
					closeResultSet(rs);
					rs = null;
					closeStatement(stmt);
					stmt = null;
					closeConnection(conn);
					conn = null;
				}
				readSuccess = true;

				try {
					conn = ds.getConnection();
					stmt = conn.prepareStatement(getUpdateSql());
					stmt.setLong(1, newValue);
					stmt.setTimestamp(2,
							new Timestamp(System.currentTimeMillis()));
					stmt.setString(3, slice);
					stmt.setLong(4, oldValue);
					int affectedRows = stmt.executeUpdate();
					if (affectedRows == 0) {
						continue;
					}

				} catch (SQLException e) {
					log.error("取范围过程中--更新出错！datasource " + index + ":" + slice,
							e);
					continue;
				} finally {
					closeStatement(stmt);
					stmt = null;
					closeConnection(conn);
					conn = null;
				}
				writeSuccess = true;
				if (readSuccess && writeSuccess)
					return new SRange(newValue, newValue + innerStep);
			}
			// 当还有最后一次重试机会时,清空excludedMap,让其有最后一次机会
			if (i == (retryTimes - 2)) {
				excludedKeyCount.clear();
			}
		}
		log.error("所有数据源都不可用！且重试" + this.retryTimes + "次后，仍然失败!");
		throw new RuntimeException("All dataSource faild to get value!");
	}

	public int getDscount() {
		return dataSources.size();
	}

	private String getInsertSql() {
		if (insertSql == null) {
			synchronized (this) {
				if (insertSql == null) {
					StringBuilder buffer = new StringBuilder();
					buffer.append("insert into ").append(getTableName())
							.append("(");
					buffer.append(getNameColumnName()).append(",");
					buffer.append(getValueColumnName()).append(",");
					buffer.append(getGmtModifiedColumnName()).append(
							") values(?,?,?);");
					insertSql = buffer.toString();
				}
			}
		}
		return insertSql;
	}

	private String getSelectSql() {
		if (selectSql == null) {
			synchronized (this) {
				if (selectSql == null) {
					StringBuilder buffer = new StringBuilder();
					buffer.append("select ").append(getValueColumnName());
					buffer.append(" from ").append(getTableName());
					buffer.append(" where ").append(getNameColumnName())
							.append(" = ?");

					selectSql = buffer.toString();
				}
			}
		}

		return selectSql;
	}

	private String getUpdateSql() {
		if (updateSql == null) {
			synchronized (this) {
				if (updateSql == null) {
					StringBuilder buffer = new StringBuilder();
					buffer.append("update ").append(getTableName());
					buffer.append(" set ").append(getValueColumnName())
							.append(" = ?, ");
					buffer.append(getGmtModifiedColumnName()).append(
							" = ? where ");
					buffer.append(getNameColumnName()).append(" = ? and ");
					buffer.append(getValueColumnName()).append(" = ?");

					updateSql = buffer.toString();
				}
			}
		}

		return updateSql;
	}

	private static void closeResultSet(ResultSet rs) {
		if (rs != null) {
			try {
				rs.close();
			} catch (SQLException e) {
				log.debug("Could not close JDBC ResultSet", e);
			} catch (Throwable e) {
				log.debug("Unexpected exception on closing JDBC ResultSet", e);
			}
		}
	}

	private static void closeStatement(Statement stmt) {
		if (stmt != null) {
			try {
				stmt.close();
			} catch (SQLException e) {
				log.debug("Could not close JDBC Statement", e);
			} catch (Throwable e) {
				log.debug("Unexpected exception on closing JDBC Statement", e);
			}
		}
	}

	private static void closeConnection(Connection conn) {
		if (conn != null) {
			try {
				conn.close();
			} catch (SQLException e) {
				log.debug("Could not close JDBC Connection", e);
			} catch (Throwable e) {
				log.debug("Unexpected exception on closing JDBC Connection", e);
			}
		}
	}

	public int getRetryTimes() {
		return retryTimes;
	}

	public void setRetryTimes(int retryTimes) {
		this.retryTimes = retryTimes;
	}

	public int getInnerStep() {
		return innerStep;
	}

	public void setInnerStep(int innerStep) {
		this.innerStep = innerStep;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public String getNameColumnName() {
		return nameColumnName;
	}

	public void setNameColumnName(String nameColumnName) {
		this.nameColumnName = nameColumnName;
	}

	public String getValueColumnName() {
		return valueColumnName;
	}

	public void setValueColumnName(String valueColumnName) {
		this.valueColumnName = valueColumnName;
	}

	public String getGmtModifiedColumnName() {
		return gmtModifiedColumnName;
	}

	public void setGmtModifiedColumnName(String gmtModifiedColumnName) {
		this.gmtModifiedColumnName = gmtModifiedColumnName;
	}

	public boolean isAdjust() {
		return adjust;
	}

	public void setAdjust(boolean adjust) {
		this.adjust = adjust;
	}

	public int getMaxSkipCount() {
		return maxSkipCount;
	}

	public void setMaxSkipCount(int maxSkipCount) {
		this.maxSkipCount = maxSkipCount;
	}

	public boolean isUseSlowProtect() {
		return useSlowProtect;
	}

	public void setUseSlowProtect(boolean useSlowProtect) {
		this.useSlowProtect = useSlowProtect;
	}

	public int getProtectMilliseconds() {
		return protectMilliseconds;
	}

	public void setProtectMilliseconds(int protectMilliseconds) {
		this.protectMilliseconds = protectMilliseconds;
	}

	public void setDataSources(List<DataSource> dataSources) {
		this.dataSources = dataSources;
	}
}
