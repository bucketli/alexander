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

import com.taobao.alexander.protocol.mysql.EOFPacket;
import com.taobao.alexander.protocol.mysql.FieldPacket;
import com.taobao.alexander.protocol.mysql.ResultSetHeaderPacket;
import com.taobao.alexander.protocol.mysql.RowDataPacket;
import com.taobao.alexander.util.IntegerUtil;
import com.taobao.alexander.util.PacketUtil;
import com.taobao.gecko.core.buffer.IoBuffer;
import com.taobao.gecko.core.core.Session;

/**
 * @author xianmao.hexm
 */
public class ShowCollation {
	private static final int FIELD_COUNT = 6;
	private static final ResultSetHeaderPacket header = PacketUtil
			.getHeader(FIELD_COUNT);
	private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
	private static final EOFPacket eof = new EOFPacket();
	static {
		int i = 0;
		byte packetId = 0;
		header.packetId = ++packetId;
		fields[i] = PacketUtil.getField("Collation", Fields.FIELD_TYPE_VAR_STRING);
		fields[i++].packetId = ++packetId;
		
		fields[i] = PacketUtil.getField("Charset", Fields.FIELD_TYPE_VAR_STRING);
		fields[i++].packetId = ++packetId;
		
		fields[i] = PacketUtil.getField("Id", Fields.FIELD_TYPE_INT24);
		fields[i++].packetId = ++packetId;
		
		fields[i] = PacketUtil.getField("Default", Fields.FIELD_TYPE_VAR_STRING);
		fields[i++].packetId = ++packetId;
		
		fields[i] = PacketUtil.getField("Compiled", Fields.FIELD_TYPE_VAR_STRING);
		fields[i++].packetId = ++packetId;
		
		fields[i] = PacketUtil.getField("Sortlen", Fields.FIELD_TYPE_INT24);
		fields[i++].packetId = ++packetId;
		eof.packetId = ++packetId;
	}

	public static void response(Session c) {
		IoBuffer buffer = IoBuffer.allocate(256);
		buffer.setAutoExpand(true);
		buffer.setAutoShrink(true);
		buffer = header.encode(buffer);
		for (FieldPacket field : fields) {
			buffer = field.encode(buffer);
		}
		buffer = eof.encode(buffer);
		
		byte packetId = eof.packetId;

		RowDataPacket row = new RowDataPacket(FIELD_COUNT);
		row.add("utf8_general_ci".getBytes());
		row.add("utf8".getBytes());
		row.add(IntegerUtil.toBytes(33));
		row.add("Yes".getBytes());
		row.add("Yes".getBytes());
		row.add(IntegerUtil.toBytes(1));
		row.packetId = ++packetId;
		buffer = row.encode(buffer);
		
		RowDataPacket row2 = new RowDataPacket(FIELD_COUNT);
		row2.add("latin1_general_ci".getBytes());
		row2.add("latin1".getBytes());
		row2.add(IntegerUtil.toBytes(48));
		row2.add("".getBytes());
		row2.add("Yes".getBytes());
		row2.add(IntegerUtil.toBytes(1));
		row2.packetId = ++packetId;
		buffer = row2.encode(buffer);
		
		RowDataPacket row3 = new RowDataPacket(FIELD_COUNT);
		row3.add("gbk_chinese_ci".getBytes());
		row3.add("gbk".getBytes());
		row3.add(IntegerUtil.toBytes(28));
		row3.add("Yes".getBytes());
		row3.add("Yes".getBytes());
		row3.add(IntegerUtil.toBytes(1));
		row3.packetId = ++packetId;
		buffer = row3.encode(buffer);
		
		RowDataPacket row4 = new RowDataPacket(FIELD_COUNT);
		row4.add("gbk_chinese_ci".getBytes());
		row4.add("gbk".getBytes());
		row4.add(IntegerUtil.toBytes(28));
		row4.add("Yes".getBytes());
		row4.add("Yes".getBytes());
		row4.add(IntegerUtil.toBytes(1));
		row4.packetId = ++packetId;
		buffer = row4.encode(buffer);

		EOFPacket lastEof = new EOFPacket();
		lastEof.packetId = ++packetId;
		buffer = lastEof.encode(buffer);
		buffer.flip();
		c.write(buffer);
	}
}
