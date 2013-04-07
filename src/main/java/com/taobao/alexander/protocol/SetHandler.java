package com.taobao.alexander.protocol;

import com.taobao.alexander.protocol.mysql.OkPacket;
import com.taobao.gecko.core.buffer.IoBuffer;
import com.taobao.gecko.core.core.Session;

/**
 * @description
 * @author <a href="junyu@taobao.com">junyu</a> 
 * @date 2013-4-7下午01:13:42
 */
public class SetHandler {
    public static void response(String sql,Session s){
    	//现在简单处理，全部返回ok包
    	IoBuffer b=IoBuffer.allocate(OkPacket.OK.length);
    	b.put(OkPacket.OK);
    	b.flip();
    	s.write(b);
    }
}
