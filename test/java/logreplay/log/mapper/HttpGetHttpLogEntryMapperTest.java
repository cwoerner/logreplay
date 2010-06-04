package logreplay.log.mapper;

import java.util.Date;

import junit.framework.TestCase;
import logreplay.log.HttpLogEntry;
import logreplay.log.mapper.HttpGetHttpLogEntryMapper;
import logreplay.log.mapper.HttpLogEntryMapper;

import org.apache.http.client.methods.HttpGet;


public class HttpGetHttpLogEntryMapperTest extends TestCase {

	protected void setUp() throws Exception {
		super.setUp();
	}

	protected void tearDown() throws Exception {
		super.tearDown();
	}

	public void testMap() { 
		HttpLogEntryMapper<HttpGet> mapper = new HttpGetHttpLogEntryMapper("http://www.unittest.com");
		HttpLogEntry httpReq = new HttpLogEntry(new Date(), "/path/to/something?x=y&z=123");
		httpReq.addHeader("Referer", "http://www.google.com");
		httpReq.addHeader("User-Agent", "Unit Test/1.0");
		httpReq.addHeader("X-Forwarded-For", "127.0.0.1,127.0.0.2");
		HttpGet get = mapper.map(httpReq);
		
		assertEquals(get.getMethod(), HttpGet.METHOD_NAME);
		
		assertEquals("www.unittest.com", get.getURI().getHost());
		assertEquals("/path/to/something", get.getURI().getPath());
		assertEquals("http", get.getURI().getScheme());
		assertEquals("x=y&z=123", get.getURI().getQuery());
	}
}
