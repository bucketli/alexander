package com.taobao.alexander.net;

import java.security.NoSuchAlgorithmException;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.alexander.protocol.Versions;
import com.taobao.alexander.protocol.mysql.AuthPacket;
import com.taobao.alexander.protocol.mysql.ErrorPacket;
import com.taobao.alexander.protocol.mysql.HandshakePacket;
import com.taobao.alexander.sequence.impl.ClustedSequenceService;
import com.taobao.alexander.util.CharsetUtil;
import com.taobao.alexander.util.EncryptUtil;
import com.taobao.alexander.util.RandomUtil;
import com.taobao.alexander.util.SecurityUtil;
import com.taobao.gecko.core.buffer.IoBuffer;
import com.taobao.gecko.core.core.Session;
import com.taobao.gecko.core.core.impl.HandlerAdapter;

/**
 * @description
 * @author <a href="junyu@taobao.com">junyu</a>
 * @date 2013-3-7下午04:06:34
 */
public class FrontConnectionHandler extends HandlerAdapter {
	public static final Log log = LogFactory
			.getLog(FrontConnectionHandler.class);
	public static final String AUTH_KEY = "AUTH_KEY";
	public static final byte[] AUTH_OK = new byte[] { 7, 0, 0, 2, 0, 0, 0, 2,
			0, 0, 0 };

	public static final String SEED = "SEED";
	public static final String DATA_HANDLER = "DATA_HANDLER";
	public static final String SERVER_VERSION = "5.1.48-sequence-1.2.3";
	public static final String AUTH_USER = "AUTH_USER";
	public static final String DEFAULT_SERVER_CHARSET_NAME = "utf-8";
	public AtomicLong id = new AtomicLong(0);
	public final ClustedSequenceService sequence;

	public FrontConnectionHandler(ClustedSequenceService sequence) {
		this.sequence = sequence;
	}

	@Override
	public void onSessionStarted(Session session) {
		if (!session.isClosed()) {
			// 生成认证数据
			byte[] rand1 = RandomUtil.randomBytes(8);
			byte[] rand2 = RandomUtil.randomBytes(12);

			// 保存认证数据
			byte[] seed = new byte[rand1.length + rand2.length];
			System.arraycopy(rand1, 0, seed, 0, rand1.length);
			System.arraycopy(rand2, 0, seed, rand1.length, rand2.length);
			session.setAttribute(SEED, seed);

			// 发送握手数据包
			HandshakePacket hs = new HandshakePacket();
			hs.packetId = 0;
			hs.protocolVersion = Versions.PROTOCOL_VERSION;
			hs.serverVersion = SERVER_VERSION.getBytes();
			hs.threadId = id.incrementAndGet();
			hs.seed = rand1;
			hs.serverCapabilities = FrontConnectionHelper
					.getServerCapabilities();
			hs.serverCharsetIndex = (byte) (CharsetUtil
					.getIndex(DEFAULT_SERVER_CHARSET_NAME) & 0xff);
			hs.serverStatus = 2;
			hs.restOfScrambleBuff = rand2;
			session.write(hs.encode().flip());
		}
	}

	@Override
	public void onMessageReceived(Session session, Object message) {
		Object auth = session.getAttribute(AUTH_KEY);
		IoBuffer buffer = (IoBuffer) message;
		if (auth != null && (Boolean) auth) {
			int length = 0;
			int offset = 0;
			for (;;) {
				length = getPacketLength(buffer, offset);
				offset += length;

				byte[] data = new byte[length];
				buffer.get(data, 0, length);
				DataHandler handler = (DataHandler) session
						.getAttribute(DATA_HANDLER);
				handler.handle(data, session);
				if (!buffer.hasRemaining()) {
					break;
				}
			}
		} else {
			int length = getPacketLength(buffer, 0);
			byte[] data = new byte[length];
			buffer.get(data, 0, length);
			processAuth(data, session);
		}
	}

	protected int getPacketLength(IoBuffer buffer, int offset) {
		int length = buffer.get(offset) & 0xff;
		length |= (buffer.get(++offset) & 0xff) << 8;
		length |= (buffer.get(++offset) & 0xff) << 16;
		return length + FrontConnectionHelper.packetHeaderSize;
	}

	private void processAuth(byte[] data, Session session) {
		AuthPacket auth = new AuthPacket();
		auth.decode(data);

		String p = sequence.getPasswd(auth.user);
		if (p == null
				|| !this.checkPasswd((byte[]) session.getAttribute(SEED), p,
						auth.password)) {
			ErrorPacket err = new ErrorPacket();
			err.packetId = (byte) 2;
			err.errno = 1044;

			err.message = ("Access denied for user '" + auth.user
					+ "' to database '" + auth.database + "'").getBytes();
			session.write(err.encode().flip());
		} else {
			session.setAttribute(DATA_HANDLER,
					new SequenceDataHandler(sequence));
			session.setAttribute(AUTH_KEY, new Boolean(true));
			session.setAttribute(AUTH_USER, auth.user);
			IoBuffer buffer = IoBuffer.allocate(AUTH_OK.length);
			buffer.put(AUTH_OK);
			buffer.flip();
			session.write(buffer);
		}
	}

	private boolean checkPasswd(byte[] seed, String serverPass, byte[] userPass) {
		// encrypt
		byte[] encryptPass = null;
		try {
			encryptPass = SecurityUtil.scramble411(serverPass.getBytes(), seed);
		} catch (NoSuchAlgorithmException e) {
			log.warn(e);
			return false;
		}
		if (!equal(encryptPass, userPass)) {
			try {
				String decryptPass = EncryptUtil.decrypt(serverPass);
				byte[] decryptPassBytes = decryptPass.getBytes();
				encryptPass = SecurityUtil.scramble411(decryptPassBytes, seed);
			} catch (NoSuchAlgorithmException e) {
				log.warn(e);
				return false;
			}
			return equal(encryptPass, userPass);
		} else {
			return true;
		}
	}

	private boolean equal(byte[] encryptPass, byte[] password) {
		if (encryptPass != null && (encryptPass.length == password.length)) {
			int i = encryptPass.length;
			while (i-- != 0) {
				if (encryptPass[i] != password[i]) {
					return false;
				}
			}
		} else {
			return false;
		}

		return true;
	}

	@Override
	public void onExceptionCaught(Session session, Throwable throwable) {
		log.error(throwable);
	}
}
