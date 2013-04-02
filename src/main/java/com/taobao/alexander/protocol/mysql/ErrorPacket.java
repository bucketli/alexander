/*
 * Copyright 1999-2012 Alibaba Group.
 *  
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *  
 *      http://www.apache.org/licenses/LICENSE-2.0
 *  
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.taobao.alexander.protocol.mysql;

import com.taobao.alexander.protocol.MySQLMessage;
import com.taobao.alexander.protocol.MySQLPacket;
import com.taobao.alexander.util.BufferUtil;
import com.taobao.gecko.core.buffer.IoBuffer;

/**
 * From server to client in response to command, if error.
 * 
 * <pre>
 * Bytes                       Name
 * -----                       ----
 * 1                           field_count, always = 0xff
 * 2                           errno
 * 1                           (sqlstate marker), always '#'
 * 5                           sqlstate (5 characters)
 * n                           message
 * 
 * @see http://forge.mysql.com/wiki/MySQL_Internals_ClientServer_Protocol#Error_Packet
 * </pre>
 * 
 * @author xianmao.hexm 2010-7-16 上午10:45:01
 */
public class ErrorPacket extends MySQLPacket {
	public static final byte FIELD_COUNT = (byte) 0xff;
	private static final byte SQLSTATE_MARKER = (byte) '#';
	private static final byte[] DEFAULT_SQLSTATE = "HY000".getBytes();

	public byte fieldCount = FIELD_COUNT;
	public int errno;
	public byte mark = SQLSTATE_MARKER;
	public byte[] sqlState = DEFAULT_SQLSTATE;
	public byte[] message;

	public static void errNo(byte[] errorPacket, int newErrno) {
		errorPacket[5] = (byte) (newErrno & 0xff);
		errorPacket[6] = (byte) ((newErrno >> 8) & 0xff);
	}

	public static void sqlStateDefault(byte[] errorPacket) {
		System.arraycopy(DEFAULT_SQLSTATE, 0, errorPacket, 8,
				DEFAULT_SQLSTATE.length);
	}

	public static int errNo(byte first, byte second) {
		int no = first & 0xff;
		no |= (second & 0xff) << 8;
		return no;
	}

	public void setDefaultSqlStat() {
		this.sqlState = DEFAULT_SQLSTATE;
	}

	@Override
	public int calcPacketSize() {
		int size = 9;// 1 + 2 + 1 + 5
		if (message != null) {
			size += message.length;
		}
		return size;
	}

	@Override
	protected String getPacketInfo() {
		return "MySQL Error Packet";
	}

	public IoBuffer encode(){
		int size = calcPacketSize();
		final IoBuffer buffer = IoBuffer.allocate(size);
		buffer.setAutoExpand(true);
		buffer.setAutoShrink(true);

        BufferUtil.writeUB3(buffer, size);
        buffer.put(packetId);
        buffer.put(fieldCount);
        BufferUtil.writeUB2(buffer, errno);
        buffer.put(mark);
        buffer.put(sqlState);
        if (message != null) {
        	buffer.put(message);
        }
        return buffer;
	}
	
	public void decode(byte[] data) {
		MySQLMessage mm = new MySQLMessage(data);
		packetLength = mm.readUB3();
		packetId = mm.read();
		fieldCount = mm.read();
		errno = mm.readUB2();
		if (mm.hasRemaining() && (mm.read(mm.position()) == SQLSTATE_MARKER)) {
			mm.read();
			sqlState = mm.readBytes(5);
		}
		message = mm.readBytes();
	}

}
