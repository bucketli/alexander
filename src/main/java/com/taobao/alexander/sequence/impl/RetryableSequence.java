package com.taobao.alexander.sequence.impl;

import java.sql.SQLException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.taobao.alexander.sequence.RangeFetcher;
import com.taobao.alexander.sequence.SRange;
import com.taobao.alexander.sequence.Sequence;

/**
 * @description
 * @author <a href="junyu@taobao.com">junyu</a>
 * @date 2013-3-7����02:30:01
 */
public class RetryableSequence implements Sequence {
	private final Lock lock = new ReentrantLock();
	private RangeFetcher fetcher;
	private String slice;
	private volatile SRange curRange;

	/**
	 * ��ʼ��һ�£����name�����ڣ�������ʼֵ
	 * 
	 * @throws SequenceException
	 * @throws SQLException
	 */
	public void init() {
		RetryableRangeFetcher groupSequenceDao = (RetryableRangeFetcher) fetcher;
		synchronized (this) // Ϊ�˱�֤��ȫ��
		{
			groupSequenceDao.adjust(slice);
		}
	}

	public long nextValue() {
		if (curRange == null) {
			lock.lock();
			try {
				if (curRange == null) {
					curRange = fetcher.nextRange(slice);
				}
			} finally {
				lock.unlock();
			}
		}

		long value = curRange.nextVal();
		if (value < 0) {
			lock.lock();
			try {
				for (;;) {
					value = curRange.nextVal();
					if (value < 0) {
						curRange = fetcher.nextRange(slice);
						continue;
					}

					break;
				}
			} finally {
				lock.unlock();
			}
		}

		if (value < 0) {
			throw new RuntimeException("Sequence value overflow,slice=" + slice
					+ ",value = " + value);
		}

		return value;
	}

	public SRange nextRange() {
		lock.lock();
		SRange range;
		try {
			range = fetcher.nextRange(slice);
		} finally {
			lock.unlock();
		}

		return range;
	}

	public RangeFetcher getFetcher() {
		return fetcher;
	}

	public void setFetcher(RangeFetcher fetcher) {
		this.fetcher = fetcher;
	}

	public String getSlice() {
		return slice;
	}

	public void setSlice(String slice) {
		this.slice = slice;
	}
}
