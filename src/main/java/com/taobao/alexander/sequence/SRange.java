package com.taobao.alexander.sequence;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @description
 * @author <a href="junyu@taobao.com">junyu</a>
 * @date 2013-3-7����01:43:13
 */
public class SRange {
	public final long startNonContain;
	public final long endContain;

	private AtomicLong curNum;

	public SRange(long startNonContain, long endContain) {
		this.startNonContain = startNonContain;
		this.endContain = endContain;
		curNum = new AtomicLong(this.startNonContain + 1);
	}

	/**
	 * -1 ��ʾ�Ѿ�ȡ��
	 * 
	 * @return
	 */
	public long nextVal() {
		long val = curNum.getAndIncrement();
		if (val <= endContain) {
			return val;
		} else {
			return -1;
		}
	}

	public long getStartContain() {
		return this.startNonContain + 1;
	}

	public long getEndContain() {
		return endContain;
	}
}
