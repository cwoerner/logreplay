package logreplay.log.parser;

import java.io.StringReader;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import junit.framework.TestCase;
import logreplay.log.HttpLogEntry;
import logreplay.log.parser.LogFileSourceParser;
import logreplay.log.parser.haproxy.HAProxyLogParser;


public class HAProxyLogParserTest extends TestCase {

	private LogFileSourceParser parser;
	
	protected void setUp() throws Exception {
		super.setUp();
		parser = new HAProxyLogParser();
	}

	protected void tearDown() throws Exception {
		super.tearDown();
	}

	public void testParseReaderEmptyData() throws ParseException {
		Iterator<HttpLogEntry> entIter = parser.parse(new StringReader(""));
		assertNotNull(entIter);
		assertFalse(entIter.hasNext());
	}
	
	public void testParseReaderBasic() throws ParseException { 
		
		List<String> logDataItems = new ArrayList<String>();
		logDataItems.add("Apr 21 21:47:10 localhost.localdomain haproxy[9898]: 127.0.0.1:26455 [21/Apr/2010:21:47:09.799] http-fe http-be/www1 0/0/99/105/239 200 390 - - ---- 37/37/32/32/0 0/0 {http://www.unittest.com/index1.html|Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; GTB0.0; .NET CLR 1.1.4322; .NET CLR 2.0.50727; MS-RTC LM 8; InfoPath.2; .NET |} \"GET / HTTP/1.1\"\n");
		logDataItems.add("Apr 21 21:47:10 localhost.localdomain haproxy[9898]: 127.0.0.2:35448 [21/Apr/2010:21:47:09.832] http-fe http-be/www1 4/0/98/104/207 200 390 - - ---- 36/36/32/32/0 0/0 {https://secure.unittest.com/index2.html|Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; GTB0.0; .NET CLR 1.1.4322; .NET CLR 2.0.50727; .NET CLR 3.0.04506.30; .NET CL|72.54.165.186} \"GET //index.html HTTP/1.1\"\n");
		logDataItems.add("Apr 21 21:47:10 localhost.localdomain haproxy[9898]: 127.0.0.3:2263 [21/Apr/2010:21:47:09.646] http-fe http-be/www1 0/0/97/115/399 200 390 - - ---- 36/36/31/31/0 0/0 {http://www.unittest.com/index3.html|Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.9.2.3) Gecko/20100401 Firefox/3.6.3|} \"GET /index2.html HTTP/1.1\"\n");
		logDataItems.add("Apr 21 21:47:10 localhost.localdomain haproxy[9898]: 127.0.0.4:51551 [21/Apr/2010:21:47:09.799] http-fe http-be/www1 0/0/99/106/255 200 390 - - CD-- 37/37/32/32/0 0/0 {http://www.unittest.com/index4.html?jsessionid=W5UTVENKKWHKVQE1GHPSKHWATMY32JVN|Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; Trident/4.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.3072|} \"GET /index.html?test=x&bar=baz HTTP/1.1\"\n");
		logDataItems.add("Apr 21 21:47:10 localhost.localdomain haproxy[9898]: 127.0.0.5:63585 [21/Apr/2010:21:47:09.779] http-fe http-be/www1 20/0/99/99/283 200 390 - - ---- 36/36/32/32/0 0/0 {http://www.unittest.com/news/showArticle.jhtml?articleID=123|Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US) AppleWebKit/533.2 (KHTML, like Gecko) Chrome/5.0.342.9 Safari/533.2|} \"GET /index.html?test=y&baz=foo HTTP/1.1\"\n");
		logDataItems.add("Apr 21 21:47:10 localhost.localdomain haproxy[9898]: 127.0.0.6:28738 [21/Apr/2010:21:47:09.513] http-fe http-be/www1 18/0/99/101/592 200 476 - - ---- 38/38/35/35/0 0/0 {http://www.unittest.comindex5.html/|Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 5.1; Trident/4.0; GTB6.4; .NET CLR 1.1.4322)|} \"GET /path/to/something HTTP/1.1\"\n");
		logDataItems.add("Apr 21 21:47:10 localhost.localdomain haproxy[9898]: 127.0.0.7:4512 [21/Apr/2010:21:47:09.810] http-fe http-be/www1 0/0/97/131/319 200 390 - - ---- 38/38/34/34/0 0/0 {http://www.unittest.com/musicians/jgarcia|Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; .NET CLR 1.1.4322; .NET CLR 2.0.50727; .NET CLR 3.0.04506.648; .NET CLR 3.5.2|} \"GET /path/to/another?with=params&other=params HTTP/1.1\"\n");
		logDataItems.add("Apr 21 21:47:10 localhost.localdomain haproxy[9898]: 127.0.0.8:55591 [21/Apr/2010:21:47:09.815] http-fe http-be/www1 3/0/97/123/317 200 390 - - ---- 37/37/33/33/0 0/0 {http://www.unittest.com/instruments/guitar|Mozilla/5.0 (Windows; U; Windows NT 6.0; en-US) AppleWebKit/532.5 (KHTML, like Gecko) Chrome/4.1.249.1045 Safari/532.5|} \"GET / HTTP/1.1\"\n");
		logDataItems.add("Apr 21 21:47:10 localhost.localdomain haproxy[9898]: 127.0.0.9:49917 [21/Apr/2010:21:47:09.891] http-fe http-be/www1 1/0/97/110/241 200 476 - - ---- 36/36/32/32/0 0/0 {http://www.unittest.com/index6.html?group=grateful_dead|Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10.5; en-US; rv:1.9.0.19) Gecko/2010031218 Firefox/3.0.19|} \"GET /musicians/ HTTP/1.1\"\n");
		logDataItems.add("Apr 21 21:47:10 localhost.localdomain haproxy[9898]: 127.0.0.10:40160 [21/Apr/2010:21:47:09.859] http-fe http-be/www1 0/0/98/103/274 200 390 - - ---- 35/35/31/31/0 0/0 {http://www.unittest.com/index7.html|Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; .NET CLR 1.1.4322; .NET CLR 2.0.50727; MS-RTC LM 8; .NET CLR 3.0.04506.6|} \"GET /programming_languages/java HTTP/1.1\"\n");
		

		StringBuffer logData = new StringBuffer();
		for (String logEnt : logDataItems) { 
			logData.append(logEnt);
		}
		
		Iterator<HttpLogEntry> entIter = parser.parse(new StringReader(logData.toString()));
		assertNotNull(entIter);
		assertTrue(entIter.hasNext());
		
		int numEntries = 0;
		while (entIter.hasNext()) { 
			HttpLogEntry curEnt = entIter.next();
			assertNotNull(curEnt.getPath());
			assertTrue(curEnt.getPath().length() > 0);
			try { 
				new URL("http://localhost" + curEnt.getPath());
			} catch (MalformedURLException e) { 
				fail("malformed url: " + e);
			}
			
			boolean hasXff = false;
			boolean hasUA = false;
			boolean hasRef = false;
			
			for (HttpLogEntry.Header hdr : curEnt.getHeaders()) { 
				if ("User-Agent".equalsIgnoreCase(hdr.name) && hdr.value != null && hdr.value.length() > 0) { 
					hasUA = true;
				}
				
				if ("Referer".equalsIgnoreCase(hdr.name) && hdr.value != null && hdr.value.length() > 0) { 
					hasRef = true;
					try { 
						new URL(hdr.value);
					} catch (MalformedURLException e) { 
						fail("malformed referer url: " + e);
					}
				}
				
				if ("X-Forwarded-For".equalsIgnoreCase(hdr.name) && hdr.value != null && hdr.value.length() > 0) { 
					hasXff = true;
					try { 
						String[] ips = hdr.value.split(",");
						for (String ip : ips) { 
							InetAddress addr = java.net.InetAddress.getByName(ip);
							assertNotNull(addr);
						}
					} catch (UnknownHostException e) { 
						fail("unknown host for xff header: " + e);
					}
				}
			}
			
			assertTrue("has xff", hasXff);
			assertTrue("has user agent", hasUA);
			assertTrue("has referer", hasRef);
			
			numEntries++;
		}
		
		assertEquals(new Integer(numEntries), new Integer(10));
	}

}
