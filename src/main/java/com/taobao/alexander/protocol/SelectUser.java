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
package com.taobao.alexander.protocol;

import java.nio.charset.Charset;

import com.taobao.alexander.net.FrontConnectionHandler;
import com.taobao.alexander.protocol.mysql.EOFPacket;
import com.taobao.alexander.protocol.mysql.FieldPacket;
import com.taobao.alexander.protocol.mysql.ResultSetHeaderPacket;
import com.taobao.alexander.protocol.mysql.RowDataPacket;
import com.taobao.alexander.util.PacketUtil;
import com.taobao.alexander.util.StringUtil;
import com.taobao.gecko.core.buffer.IoBuffer;
import com.taobao.gecko.core.core.Session;

/**
 * @author xianmao.hexm
 */
public class SelectUser {
	private static final int FIELD_COUNT = 1;
	private static final ResultSetHeaderPacket header = PacketUtil
			.getHeader(FIELD_COUNT);
	private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
	private static final EOFPacket eof = new EOFPacket();
	static {
		int i = 0;
		byte packetId = 0;
		header.packetId = ++packetId;
		fields[i] = PacketUtil.getField("USER()", Fields.FIELD_TYPE_VAR_STRING);
		fields[i++].packetId = ++packetId;
		eof.packetId = ++packetId;
	}

	public static void response(Session c) {
		IoBuffer buffer = IoBuffer.allocate(10);
		buffer.setAutoExpand(true);
		buffer.setAutoShrink(true);
		buffer = header.encode(buffer);
		for (FieldPacket field : fields) {
			buffer = field.encode(buffer);
		}
		buffer = eof.encode(buffer);
		byte packetId = eof.packetId;
		RowDataPacket row = new RowDataPacket(FIELD_COUNT);
		row.add(getUser(c));
		row.packetId = ++packetId;
		buffer = row.encode(buffer);
		EOFPacket lastEof = new EOFPacket();
		lastEof.packetId = ++packetId;
		buffer = lastEof.encode(buffer);
		buffer.flip();
		c.write(buffer);
	}

	private static byte[] getUser(Session c) {
		StringBuilder sb = new StringBuilder();
		sb.append(c.getAttribute(FrontConnectionHandler.AUTH_USER)).append('@')
				.append("dummy_ip");
		return StringUtil
				.encode(sb.toString(), Charset.defaultCharset().name());
	}

}
