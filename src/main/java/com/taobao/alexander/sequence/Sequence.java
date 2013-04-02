package com.taobao.alexander.sequence;
/**
 * @description
 * @author <a href="junyu@taobao.com">junyu</a> 
 * @date 2013-3-7обнГ01:41:53
 */
public interface Sequence {
    public long nextValue();
    
    public SRange nextRange();
}
