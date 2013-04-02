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
 * @date 2013-3-7����02:30:42
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
	 * ����Դ
	 */
	private List<DataSource> dataSources;

	/**
	 * ����Ӧ����
	 */
	private boolean adjust = DEFAULT_ADJUST;
	/**
	 * ���Դ���
	 */
	private int retryTimes = DEFAULT_RETRY_TIMES;

	/**
	 * �ڲ���
	 */
	private int innerStep = DEFAULT_INNER_STEP;

	/**
	 * �ⲽ��
	 */
	private int outStep = DEFAULT_INNER_STEP;

	/**
	 * �������ڵı���
	 */
	private String tableName = DEFAULT_TABLE_NAME;

	/**
	 * �洢�������Ƶ�����
	 */
	private String nameColumnName = DEFAULT_NAME_COLUMN_NAME;

	/**
	 * �洢����ֵ������
	 */
	private String valueColumnName = DEFAULT_VALUE_COLUMN_NAME;

	/**
	 * �洢����������ʱ�������
	 */
	private String gmtModifiedColumnName = DEFAULT_GMT_MODIFIED_COLUMN_NAME;

	private volatile String selectSql;
	private volatile String updateSql;
	private volatile String insertSql;

	/**
	 * ���Ի�
	 * 
	 * @throws SequenceException
	 */
	public void init() {
		if (dataSources == null) {
			log.error("����sequence��datasource mapΪ��");
			throw new RuntimeException("����sequence��datasource mapΪ��");
		}

		outStep = innerStep * (dataSources.size());// �����ⲽ��
		StringBuilder sb = new StringBuilder();
		sb.append("GroupSequenceDao��ʼ����ɣ�\r\n ");
		sb.append("innerStep:").append(this.innerStep).append("\r\n");
		sb.append("dataSource size:").append(dataSources.size()).append("��:");
		sb.append("\r\n");
		sb.append("adjust��").append(adjust).append("\r\n");
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
	 *            gourp�ڵ���ţ���0��ʼ
	 * @param value
	 *            ��ǰȡ��ֵ
	 * @return
	 */
	private boolean check(int index, long value) {
		return (value % outStep) == (index * innerStep);
	}

	/**
	 * ��鲢����ĳ��sequence 1�����sequece�����ڣ�����ֵ������ʼ��ֵ 2������Ѿ����ڣ������ص�����������
	 * 3������Ѿ����ڣ������ص���
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
					if (!check(i, val)) // �����ֵ
					{
						if (this.isAdjust()) {
							this.adjustUpdate(i, val, slice);
						} else {
							log.error("���ݿ������õĳ�ֵ���������������ݿ⣬��������adjust����");
							throw new RuntimeException(
									"���ݿ������õĳ�ֵ���������������ݿ⣬��������adjust����");
						}
					}
				}
				if (item == 0)// ������,����������¼
				{
					if (this.isAdjust()) {
						this.adjustInsert(i, slice);
					} else {
						log.error("���ݿ���δ���ø�sequence���������ݿ��в���sequence��¼����������adjust����");
						throw new RuntimeException(
								"���ݿ���δ���ø�sequence���������ݿ��в���sequence��¼����������adjust����");
					}
				}
			} catch (SQLException e) {// �̵�SQL�쳣�������������õĿ����
				log.error("��ֵУ�������Ӧ�����г���.", e);
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
	 * ����
	 * 
	 * @param index
	 * @param value
	 * @param slice
	 * @throws SequenceException
	 * @throws SQLException
	 */
	private void adjustUpdate(int index, long value, String slice)
			throws SQLException {
		long newValue = (value - value % outStep) + outStep + index * innerStep;// ���ó��µĵ���ֵ
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
			log.info("datasource " + index + " ���³�ֵ�ɹ�!" + "sequence Name��"
					+ slice + "���¹��̣�" + value + "-->" + newValue);
		} catch (SQLException e) { // �Ե�SQL�쳣����Sequence�쳣
			log.error("����SQLException,���³�ֵ����Ӧʧ�ܣ�dbGroupIndex:datasource "
					+ index + ",sequence Name��" + slice + "���¹��̣�" + value
					+ "-->" + newValue, e);
			throw new RuntimeException(
					"����SQLException,���³�ֵ����Ӧʧ�ܣ�dbGroupIndex:+datasource+"
							+ index + ",sequence Name��" + slice + "���¹��̣�"
							+ value + "-->" + newValue, e);
		} finally {
			closeStatement(stmt);
			stmt = null;
			closeConnection(conn);
			conn = null;
		}
	}

	/**
	 * ������ֵ
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
			log.info("datasource " + index + "   name:" + slice + "�����ֵ:"
					+ slice + "value:" + newValue);

		} catch (SQLException e) { // �Ե�SQL�쳣����sequence�쳣
			log.error("����SQLException,�����ֵ����Ӧʧ�ܣ�dbGroupIndex:datasource "
					+ index + ",sequence Name��" + slice + "   value:"
					+ newValue, e);
			throw new RuntimeException(
					"����SQLException,�����ֵ����Ӧʧ�ܣ�dbGroupIndex:datasource " + index
							+ "sequence Name��" + slice + "   value:" + newValue,
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

	private ConcurrentHashMap<Integer/* ds index */, AtomicInteger/* �ӹ����� */> excludedKeyCount = new ConcurrentHashMap<Integer, AtomicInteger>();
	// ����Թ�������ָ�
	private int maxSkipCount = 10;
	// ʹ���������ݿⱣ��
	private boolean useSlowProtect = false;
	// ������ʱ��
	private int protectMilliseconds = 50;

	private ExecutorService exec = Executors.newFixedThreadPool(1);

	public SRange nextRange(final String slice) throws RuntimeException {
		if (slice == null) {
			log.error("������Ϊ�գ�");
			throw new IllegalArgumentException("�������Ʋ���Ϊ��");
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
						log.error(maxSkipCount + "�����ѹ���indexΪ" + index
								+ "������Դ�������³���ȡ����");
					} else {
						continue;
					}
				}

				final DataSource ds = dataSources.get(index);
				// ��ѯ��ֻ�����������ݿ�ҵ��������������ݿⱣ��
				try {
					// ���δʹ�ó�ʱ���������Ѿ�ֻʣ����1������Դ��������ô��ȥ��
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
										// ֱ���׳��쳣����ӣ�����������Ҫֱ�ӹر�����
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
									"[SEQUENCE SLOW-PROTECTED MODE]:TimeoutException,��ǰ���ó�ʱʱ��Ϊ"
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
					if (!check(index, newValue)) // ���������ֵ������
					{
						if (this.isAdjust()) {
							newValue = (newValue - newValue % outStep)
									+ outStep + index * innerStep;// ���ó��µĵ���ֵ
						} else {
							RuntimeException sequenceException = new RuntimeException(
									"datasource "
											+ index
											+ ":"
											+ slice
											+ "��ֵ�ô��󣬸��ǵ�������Χ���ˣ����޸����ݿ⣬���߿���adjust���أ�");

							log.error("datasource" + index + ":" + slice
									+ "��ֵ�ô��󣬸��ǵ�������Χ���ˣ����޸����ݿ⣬���߿���adjust���أ�",
									sequenceException);
							throw sequenceException;
						}
					}
				} catch (SQLException e) {
					log.error("ȡ��Χ������--��ѯ����datasource " + i + ":" + slice, e);
					// �������Դֻʣ�������һ�����Ͳ�Ҫ�ų���
					if (excludedKeyCount.size() < (dataSources.size() - 1)) {
						excludedKeyCount.put(index, new AtomicInteger(0));
						log.error("��ʱ�߳�indexΪ" + index + "������Դ��" + maxSkipCount
								+ "�κ����³���");
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
					log.error("ȡ��Χ������--���³���datasource " + index + ":" + slice,
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
			// ���������һ�����Ի���ʱ,���excludedMap,���������һ�λ���
			if (i == (retryTimes - 2)) {
				excludedKeyCount.clear();
			}
		}
		log.error("��������Դ�������ã�������" + this.retryTimes + "�κ���Ȼʧ��!");
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
