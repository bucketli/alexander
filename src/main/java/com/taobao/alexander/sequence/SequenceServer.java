package com.taobao.alexander.sequence;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.util.Properties;

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
		TCPController controller=new TCPController(loadServerConfig());
		controller.setLocalSocketAddress(new InetSocketAddress(8507));
		controller.setHandler(handler);
		controller.start();
	}
	
	public static final String SERVER_CONFIG_FILE_NAME="server.properties";
	private Configuration loadServerConfig() throws IOException{
		Configuration conf=new Configuration();
		conf.setReadThreadCount(Runtime.getRuntime().availableProcessors()-1);
		conf.setWriteThreadCount(Runtime.getRuntime().availableProcessors()-1);
		conf.setDispatchMessageThreadCount(Runtime.getRuntime().availableProcessors()-1);
		//not to check the session timeout
		conf.setSessionIdleTimeout(0);
		conf.setSessionReadBufferSize(512);
		
		InputStream is=SequenceServer.class.getClassLoader().getResourceAsStream(SERVER_CONFIG_FILE_NAME);
		if(is==null){
			is=ClassLoader.getSystemResourceAsStream(SERVER_CONFIG_FILE_NAME);
		}
		
		
		if(is!=null){
			Properties prop=new Properties();
			try {
				prop.load(is);
			} catch (IOException e) {
				throw e;
			} finally{
				is.close();
			}
			
			String sRead=prop.getProperty("read_thread_count");
			String sWrite=prop.getProperty("write_thread_count");
			String sDispatch=prop.getProperty("dispatch_thread_count");
			String sIdle=prop.getProperty("session_idle_timeout");
			String sReadBuf=prop.getProperty("read_buffer_size");
			
			if(sRead!=null&&!"".equals(sRead.trim())){
				conf.setReadThreadCount(Integer.valueOf(sRead));
			}
			
			if(sWrite!=null&&!"".equals(sWrite.trim())){
				conf.setWriteThreadCount(Integer.valueOf(sWrite));
			}
			
			if(sDispatch!=null&&!"".equals(sDispatch.trim())){
				conf.setDispatchMessageThreadCount(Integer.valueOf(sDispatch));
			}
			
			if(sIdle!=null&&!"".equals(sIdle.trim())){
				conf.setSessionIdleTimeout(Integer.valueOf(sIdle));
			}
			
			if(sReadBuf!=null&&!"".equals(sReadBuf.trim())){
				conf.setSessionReadBufferSize(Integer.valueOf(sReadBuf));
			}
		}
		
		return conf;
	}
}
