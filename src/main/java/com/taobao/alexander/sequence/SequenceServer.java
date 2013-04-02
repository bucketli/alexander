package com.taobao.alexander.sequence;

import java.io.IOException;
import java.net.InetSocketAddress;

import com.taobao.alexander.net.FrontConnectionHandler;
import com.taobao.alexander.sequence.impl.ClustedSequenceService;
import com.taobao.gecko.core.nio.TCPController;

/**
 * @description
 * @author <a href="junyu@taobao.com">junyu</a>
 * @date 2013-3-7ÏÂÎç03:47:54
 */
public class SequenceServer{
	public static void main(String[] args) throws IOException {
		SequenceServer server = new SequenceServer();
		server.startUp();
	}

	public void startUp() throws IOException {
		ClustedSequenceService sequence = new ClustedSequenceService();
		sequence.init();
		FrontConnectionHandler handler=new FrontConnectionHandler(sequence);
		TCPController controller=new TCPController();
		controller.setLocalSocketAddress(new InetSocketAddress(8507));
		controller.setHandler(handler);
		controller.start();
	}
}
