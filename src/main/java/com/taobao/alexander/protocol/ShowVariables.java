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
import com.taobao.alexander.util.PacketUtil;
import com.taobao.gecko.core.buffer.IoBuffer;
import com.taobao.gecko.core.core.Session;

/**
 * @author xianmao.hexm
 */
public class ShowVariables {
	private static final int FIELD_COUNT = 2;
	private static final ResultSetHeaderPacket header = PacketUtil
			.getHeader(FIELD_COUNT);
	private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
	private static final EOFPacket eof = new EOFPacket();
	static {
		int i = 0;
		byte packetId = 0;
		header.packetId = ++packetId;
		fields[i] = PacketUtil.getField("Variable_name", Fields.FIELD_TYPE_VAR_STRING);
		fields[i++].packetId = ++packetId;
		
		fields[i] = PacketUtil.getField("Value", Fields.FIELD_TYPE_VAR_STRING);
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
//		
//		RowDataPacket row = new RowDataPacket(FIELD_COUNT);
//		row.add("character_set_client".getBytes());
//		row.add("utf8".getBytes());
//		row.packetId = ++packetId;
//		buffer = row.encode(buffer);
//		
//		RowDataPacket row2 = new RowDataPacket(FIELD_COUNT);
//		row2.add("character_set_connection".getBytes());
//		row2.add("utf8".getBytes());
//		row2.packetId = ++packetId;
//		buffer = row.encode(buffer);
//		
//		RowDataPacket row3 = new RowDataPacket(FIELD_COUNT);
//		row3.add("character_set_results".getBytes());
//		row3.add("utf8".getBytes());
//		row3.packetId = ++packetId;
//		buffer = row.encode(buffer);
//		
//		RowDataPacket row4 = new RowDataPacket(FIELD_COUNT);
//		row4.add("character_set_server".getBytes());
//		row4.add("utf8".getBytes());
//		row4.packetId = ++packetId;
//		buffer = row.encode(buffer);
//		
//		RowDataPacket row5 = new RowDataPacket(FIELD_COUNT);
//		row5.add("init_connect".getBytes());
//		row5.add("".getBytes());
//		row5.packetId = ++packetId;
//		buffer = row.encode(buffer);
//		
//		RowDataPacket row6 = new RowDataPacket(FIELD_COUNT);
//		row6.add("interactive_timeout".getBytes());
//		row6.add("28800".getBytes());
//		row6.packetId = ++packetId;
//		buffer = row.encode(buffer);
//		
//		RowDataPacket row7 = new RowDataPacket(FIELD_COUNT);
//		row7.add("language".getBytes());
//		row7.add("/home".getBytes());
//		row7.packetId = ++packetId;
//		buffer = row.encode(buffer);
//		
//		RowDataPacket row8 = new RowDataPacket(FIELD_COUNT);
//		row8.add("lower_case_table_names".getBytes());
//		row8.add("1".getBytes());
//		row8.packetId = ++packetId;
//		buffer = row.encode(buffer);
		
		EOFPacket lastEof = new EOFPacket();
		lastEof.packetId = ++packetId;
		buffer = lastEof.encode(buffer);
		buffer.flip();
		c.write(buffer);
	}
}
