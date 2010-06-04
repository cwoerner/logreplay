package logreplay.producer;

import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import junit.framework.TestCase;
import logreplay.log.mapper.HttpGetHttpLogEntryMapper;
import logreplay.log.parser.haproxy.HAProxyLogParser;
import logreplay.log.source.LogFileSource;
import logreplay.log.tracker.LoggingLogFileSourceTracker;
import logreplay.producer.FillException;
import logreplay.producer.HttpGetProducerImpl;
import logreplay.producer.Producer;

import org.apache.http.client.methods.HttpGet;


public class ProducerImplTest extends TestCase {

	protected void setUp() throws Exception {
		super.setUp();
	}

	protected void tearDown() throws Exception {
		super.tearDown();
	}

	public void testEmptySources() {
		List<LogFileSource> sources = new ArrayList<LogFileSource>();
		
		Producer<HttpGet> producer = new HttpGetProducerImpl(new HttpGetHttpLogEntryMapper("http://localhost"), 
				sources.iterator(), new LoggingLogFileSourceTracker());

		try { 
			producer.start();
			fail("expected no such element exception");
		} catch (Throwable t) { 
			assertTrue(t instanceof FillException);
		}
	}

	public void testGetQueueDepth() throws FillException { 
		Producer<?> producer = createTestProducer();
		producer.start();
		assertEquals(producer.getQueueDepth(), 3);
		assertFalse(producer.isExhausted());
	}

	public void testTake() throws Exception {
		
		Producer<HttpGet> producer = createTestProducer();
		producer.start();
		long i = 0;
		long depth = producer.getQueueDepth();
		for (; i < depth; i++) { 
			HttpGet get = producer.take();
			assertNotNull(get);
		}
		assertTrue(producer.isExhausted());
		assertEquals(new Long(i), new Long(depth));
	}

	
	private Producer<HttpGet> createTestProducer() { 

		StringBuilder sb = new StringBuilder();
		sb.append("Apr 21 21:47:10 localhost.localdomain haproxy[9898]: 204.29.15.2:26455 [21/Apr/2010:21:47:09.799] leads-http-fe leads-http-be/www1 0/0/99/105/239 200 390 - - ---- 37/37/32/32/0 0/0 {http://www.demandbase.com/directory/davis_street_land_co_of_tennessee_llc-tn-nashville-6519-business-contacts|Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; GTB0.0; .NET CLR 1.1.4322; .NET CLR 2.0.50727; MS-RTC LM 8; InfoPath.2; .NET |} \"GET /in.php?site_id=32783&res=1600x1200&lang=en&secure=0&href=%2Fdirectory%2Fdavis_street_land_co_of_tennessee_llc-tn-nashville-6519-business-contacts&title=Davis%20Street%20Land%20Co%20Of%20Tennessee%20Llc%2C%20Nashville%20TN%20business%20contact%20information%20and%20company%20information%20from%20Demandbase&ref=&jsuid=3376167984378186865&mime=js&x=0.7273904391287356 HTTP/1.1\"\n");
		sb.append("Apr 21 21:47:10 localhost.localdomain haproxy[9898]: 127.0.0.1:35448 [21/Apr/2010:21:47:09.832] leads-http-fe leads-http-be/www1 4/0/98/104/207 200 390 - - ---- 36/36/32/32/0 0/0 {https://secure.gallerycollection.com/customizeorder/?step=5|Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; GTB0.0; .NET CLR 1.1.4322; .NET CLR 2.0.50727; .NET CLR 3.0.04506.30; .NET CL|72.54.165.186} \"GET /in.php?site_id=179651&res=1280x800&lang=en&secure=1&href=%2Fcustomizeorder%2F%3Fstep%3D5&title=The%20Gallery%20Collection&ref=&jsuid=1841384365995162438&mime=js&x=0.519074862698212 HTTP/1.1\"\n");
		sb.append("Apr 21 21:47:10 localhost.localdomain haproxy[9898]: 161.231.132.14:2263 [21/Apr/2010:21:47:09.646] leads-http-fe leads-http-be/www1 0/0/97/115/399 200 390 - - ---- 36/36/31/31/0 0/0 {http://www.zetta.net/aboutUs.php|Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.9.2.3) Gecko/20100401 Firefox/3.6.3|} \"GET /in.php?site_id=98373&type=ping&jsuid=3541741274080471221&mime=js&x=0.7638588032561825 HTTP/1.1\"\n");

		final String logData = sb.toString();
		
		List<LogFileSource> sources = new ArrayList<LogFileSource>();
		sources.add(new LogFileSource(UUID.randomUUID().toString(), new HAProxyLogParser()) { 
			public Reader getReader() { 
				return new StringReader(logData);
			}
		});
		
		return new HttpGetProducerImpl(new HttpGetHttpLogEntryMapper("http://localhost"), 
				sources.iterator(), new LoggingLogFileSourceTracker());
	}
	
}
