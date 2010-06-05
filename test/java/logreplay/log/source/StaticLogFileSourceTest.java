package logreplay.log.source;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.util.Arrays;


import junit.framework.TestCase;
import logreplay.log.parser.haproxy.HAProxyLogParser;
import logreplay.log.source.LogFileSource;
import logreplay.log.source.StaticLogFileSource;

public class StaticLogFileSourceTest extends TestCase {

	protected void setUp() throws Exception {
		super.setUp();
	}

	protected void tearDown() throws Exception {
		super.tearDown();
	}

	public void testGetReader() throws IOException {
		
		StringBuilder sb = new StringBuilder();
		sb.append("Apr 21 21:47:10 localhost.localdomain haproxy[9898]: 127.0.0.1:26455 [21/Apr/2010:21:47:09.799] http-fe http-be/www1 0/0/99/105/239 200 390 - - ---- 37/37/32/32/0 0/0 {http://www.unittest.com/index1.html|Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; GTB0.0; .NET CLR 1.1.4322; .NET CLR 2.0.50727; MS-RTC LM 8; InfoPath.2; .NET |} \"GET / HTTP/1.1\"\n");
		sb.append("Apr 21 21:47:10 localhost.localdomain haproxy[9898]: 127.0.0.2:35448 [21/Apr/2010:21:47:09.832] http-fe http-be/www1 4/0/98/104/207 200 390 - - ---- 36/36/32/32/0 0/0 {https://secure.unittest.com/index2.html|Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; GTB0.0; .NET CLR 1.1.4322; .NET CLR 2.0.50727; .NET CLR 3.0.04506.30; .NET CL|72.54.165.186} \"GET //index.html HTTP/1.1\"\n");
		sb.append("Apr 21 21:47:10 localhost.localdomain haproxy[9898]: 127.0.0.3:2263 [21/Apr/2010:21:47:09.646] http-fe http-be/www1 0/0/97/115/399 200 390 - - ---- 36/36/31/31/0 0/0 {http://www.unittest.com/index3.html|Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.9.2.3) Gecko/20100401 Firefox/3.6.3|} \"GET /index2.html HTTP/1.1\"\n");
		
		String logData = sb.toString();
		
		File tmp = File.createTempFile(getClass().getSimpleName(), ".txt");
		tmp.deleteOnExit();
		
		FileWriter writer = new FileWriter(tmp);
		writer.write(logData);
		writer.flush();
		writer.close();
		
		String path = tmp.getAbsolutePath();
		LogFileSource src = new StaticLogFileSource(path, new HAProxyLogParser());
		Reader reader = src.getReader();
		assertNotNull(reader);
		
		sb = new StringBuilder();
		int len = 1024;
		char[] cbuf = new char[len];
		int bytesRead=0;
		while ((bytesRead = reader.read(cbuf, 0, len)) > 0) { 
			sb.append(Arrays.copyOf(cbuf, bytesRead));
		}
		reader.close();
		
		assertEquals(sb.toString(), logData, sb.toString());
	}
	
	public void testFileNameConstructor() throws IOException { 
		File tmp = File.createTempFile("unittest", ".txt");
		tmp.deleteOnExit();
		LogFileSource src = new StaticLogFileSource(tmp, new HAProxyLogParser());
		assertEquals(tmp.getCanonicalPath(), src.getId());
		assertTrue(new File(src.getId()).exists());
	}
	
	public void testFileNameConstructorInvalidFileFails() throws IOException { 
		File tmp = File.createTempFile("unittest", ".txt");
		tmp.delete();
		
		try { 
			new StaticLogFileSource(tmp, new HAProxyLogParser());
		} catch (Throwable t) { 
			assertTrue(t.toString(), t instanceof IOException);
		}
	}
}
