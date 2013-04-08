package com.taobao.alexander.net;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.alexander.protocol.Fields;
import com.taobao.alexander.protocol.mysql.EOFPacket;
import com.taobao.alexander.protocol.mysql.FieldPacket;
import com.taobao.alexander.protocol.mysql.ResultSetHeaderPacket;
import com.taobao.alexander.protocol.mysql.RowDataPacket;
import com.taobao.alexander.util.LongUtil;
import com.taobao.alexander.util.PacketUtil;
import com.taobao.gecko.core.buffer.IoBuffer;
import com.taobao.gecko.core.core.Session;

/**
 * @description
 * @author <a href="junyu@taobao.com">junyu</a>
 * @date 2012-10-10下午06:44:20
 */
public class NextVal {
	public static Log log=LogFactory.getLog(NextVal.class);
	private static final int FIELD_COUNT = 3;
	private static final ResultSetHeaderPacket header = PacketUtil
			.getHeader(FIELD_COUNT);
	private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
	private static final EOFPacket eof = new EOFPacket();
	public static final int NEXT_VAL_MAX_PACKAGE_SIZE=194;//5(header)+8(extra)+95(fields)+9(eof)+30(max clustername)+30(max slicename)+8(64bit value)+9(eof)
	
	static {
		int i = 0;
		byte packetId = 0;
		header.packetId = ++packetId;

		fields[i] = PacketUtil.getField("CLUSTER", Fields.FIELD_TYPE_VARCHAR);
		fields[i++].packetId = ++packetId;

		fields[i] = PacketUtil.getField("SLICE", Fields.FIELD_TYPE_VARCHAR);
		fields[i++].packetId = ++packetId;

		fields[i] = PacketUtil.getField("VALUE", Fields.FIELD_TYPE_LONGLONG);
		fields[i++].packetId = ++packetId;

		eof.packetId = ++packetId;
	}

	public static void send(Session c, String cluster, String slice, long value) {
		IoBuffer buffer = IoBuffer.allocate(NextVal.NEXT_VAL_MAX_PACKAGE_SIZE);
		// write header
		buffer = header.encode(buffer);
	
		// write fields
		for (FieldPacket field : fields) {
			buffer = field.encode(buffer);
		}
        
		// write eof
		buffer = eof.encode(buffer);

		// row data
		byte packetId = eof.packetId;

		RowDataPacket row = getRow(cluster, slice, value);
		row.packetId = ++packetId;
		buffer = row.encode(buffer);

		EOFPacket lastEof = new EOFPacket();
		lastEof.packetId = ++packetId;
		buffer = lastEof.encode(buffer);

		// write buffer
		buffer.flip();
		c.write(buffer);
	}

	public static RowDataPacket getRow(String cluster, String slice, long value) {
		RowDataPacket row = new RowDataPacket(FIELD_COUNT);
		row.add(cluster.getBytes());
		row.add(slice.getBytes());
		row.add(LongUtil.toBytes(value));
		return row;
	}
}
