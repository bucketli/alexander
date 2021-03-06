package com.taobao.alexander.net;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.alexander.protocol.Fields;
import com.taobao.alexander.protocol.mysql.EOFPacket;
import com.taobao.alexander.protocol.mysql.FieldPacket;
import com.taobao.alexander.protocol.mysql.ResultSetHeaderPacket;
import com.taobao.alexander.protocol.mysql.RowDataPacket;
import com.taobao.alexander.sequence.SRange;
import com.taobao.alexander.util.LongUtil;
import com.taobao.alexander.util.PacketUtil;
import com.taobao.gecko.core.buffer.IoBuffer;
import com.taobao.gecko.core.core.Session;

/**
 * @description
 * @author <a href="junyu@taobao.com">junyu</a>
 * @date 2012-10-10下午06:44:20
 */
public class NextRange {
	public static Log log=LogFactory.getLog(NextRange.class);
	private static final int FIELD_COUNT = 4;
	private static final ResultSetHeaderPacket header = PacketUtil
			.getHeader(FIELD_COUNT);
	private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
	private static final EOFPacket eof = new EOFPacket();
	public static final int NEXT_RANGE_MAX_PACKAGE_SIZE=231;//5(header)+8(extra)+124(fields)+9(eof)+30(max clustername)+30(max slicename)+8(start,64bit)+8(end,64bit)+9(eof);

	static {
		int i = 0;
		byte packetId = 0;
		header.packetId = ++packetId;

		fields[i] = PacketUtil.getField("CLUSTER", Fields.FIELD_TYPE_VARCHAR);
		fields[i++].packetId = ++packetId;

		fields[i] = PacketUtil.getField("SLICE", Fields.FIELD_TYPE_VARCHAR);
		fields[i++].packetId = ++packetId;

		fields[i] = PacketUtil.getField("START", Fields.FIELD_TYPE_LONGLONG);
		fields[i++].packetId = ++packetId;
		
		fields[i] = PacketUtil.getField("END", Fields.FIELD_TYPE_LONGLONG);
		fields[i++].packetId = ++packetId;

		eof.packetId = ++packetId;
	}

	public static void send(Session c, String cluster, String slice,SRange r) {
		IoBuffer buffer = IoBuffer.allocate(NextRange.NEXT_RANGE_MAX_PACKAGE_SIZE);

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

		RowDataPacket row = getRow(cluster, slice, r);
		row.packetId = ++packetId;
		buffer = row.encode(buffer);

		EOFPacket lastEof = new EOFPacket();
		lastEof.packetId = ++packetId;
		buffer = lastEof.encode(buffer);
		// write buffer
		buffer.flip();
		c.write(buffer);
	}

	public static RowDataPacket getRow(String cluster, String slice, SRange s) {
		RowDataPacket row = new RowDataPacket(FIELD_COUNT);
		row.add(cluster.getBytes());
		row.add(slice.getBytes());
		row.add(LongUtil.toBytes(s.getStartContain()));
		row.add(LongUtil.toBytes(s.getEndContain()));
		return row;
	}
}
