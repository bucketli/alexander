package com.taobao.alexander.sequence;

/**
 * @description
 * @author <a href="junyu@taobao.com">junyu</a>
 * @date 2013-3-7обнГ02:11:49
 */
public interface RangeFetcher {
	SRange nextRange(String slice);
}
