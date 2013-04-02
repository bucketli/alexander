package com.taobao.alexander.net;

import com.taobao.gecko.core.core.Session;

/**
 * @description
 * @author <a href="junyu@taobao.com">junyu</a> 
 * @date 2013-3-7обнГ05:24:29
 */
public interface DataHandler {
    public void handle(byte[] data,Session source);
}
