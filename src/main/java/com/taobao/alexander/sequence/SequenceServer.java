package com.taobao.alexander.sequence;

import java.io.IOException;
import java.net.InetSocketAddress;

import com.taobao.alexander.net.FrontConnectionHandler;
import com.taobao.alexander.sequence.impl.ClustedSequenceService;
import com.taobao.gecko.core.config.Configuration;
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
		Configuration conf=new Configuration();
		conf.setReadThreadCount(Runtime.getRuntime().availableProcessors()-1);
		conf.setWriteThreadCount(Runtime.getRuntime().availableProcessors()-1);
		conf.setDispatchMessageThreadCount(Runtime.getRuntime().availableProcessors()-1);
		conf.setWriteThreadCount(Runtime.getRuntime().availableProcessors()-1);
		//not to check the session timeout
		conf.setSessionIdleTimeout(0);
		TCPController controller=new TCPController(conf);
		controller.setLocalSocketAddress(new InetSocketAddress(8507));
		controller.setHandler(handler);
		controller.start();
	}
}
