package logreplay;

import java.io.File;
import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import junit.framework.TestCase;
import logreplay.HAProxyLogReplayer;
import logreplay.HttpGetRequestRunner;
import logreplay.log.HttpLogEntry;
import logreplay.log.LogFileSourceIterator;
import logreplay.log.SequentialLogFileSourceIterator;
import logreplay.log.parser.haproxy.HAProxyLogParser;
import logreplay.log.source.LogFileSource;
import logreplay.log.tracker.LogFileSourceTracker;
import logreplay.log.tracker.RenamingStaticLogFileSourceTracker;
import logreplay.producer.Producer;

import org.apache.http.client.methods.HttpGet;


public class HAProxyLogReplayerTest extends TestCase {

	private static final HAProxyLogParser parser = new HAProxyLogParser();

	private List<String> logDataItems;
	private List<String> logDataItems2;
	private LogFileSource[] logFileSources;
	
	protected void setUp() throws Exception {
		super.setUp();
		
		logDataItems = new ArrayList<String>();
		logDataItems.add("Apr 21 21:47:10 localhost.localdomain haproxy[9898]: 71.98.63.188:28738 [21/Apr/2010:21:47:09.513] leads-http-fe leads-http-be/www1 18/0/99/101/592 200 476 - - ---- 38/38/35/35/0 0/0 {http://www.marketingexperiments.com/|Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 5.1; Trident/4.0; GTB6.4; .NET CLR 1.1.4322)|} \"GET /in.php?site_id=103061&res=1440x900&lang=en&secure=0&href=%2F&title=Discover%20Which%20Marketing%20Programs%20Really%20Work&ref=&jsuid=2560584024606250453&mime=js&x=0.5277149046036997 HTTP/1.1\"\n");
		logDataItems.add("Apr 21 21:47:10 localhost.localdomain haproxy[9898]: 161.231.132.14:2263 [21/Apr/2010:21:47:09.646] leads-http-fe leads-http-be/www1 0/0/97/115/399 200 390 - - ---- 36/36/31/31/0 0/0 {http://www.zetta.net/aboutUs.php|Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.9.2.3) Gecko/20100401 Firefox/3.6.3|} \"GET /in.php?site_id=98373&type=ping&jsuid=3541741274080471221&mime=js&x=0.7638588032561825 HTTP/1.1\"\n");
		logDataItems.add("Apr 21 21:47:10 localhost.localdomain haproxy[9898]: 204.29.15.2:26455 [21/Apr/2010:21:47:09.799] leads-http-fe leads-http-be/www1 0/0/99/105/239 200 390 - - ---- 37/37/32/32/0 0/0 {http://www.demandbase.com/directory/davis_street_land_co_of_tennessee_llc-tn-nashville-6519-business-contacts|Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; GTB0.0; .NET CLR 1.1.4322; .NET CLR 2.0.50727; MS-RTC LM 8; InfoPath.2; .NET |} \"GET /in.php?site_id=32783&res=1600x1200&lang=en&secure=0&href=%2Fdirectory%2Fdavis_street_land_co_of_tennessee_llc-tn-nashville-6519-business-contacts&title=Davis%20Street%20Land%20Co%20Of%20Tennessee%20Llc%2C%20Nashville%20TN%20business%20contact%20information%20and%20company%20information%20from%20Demandbase&ref=&jsuid=3376167984378186865&mime=js&x=0.7273904391287356 HTTP/1.1\"\n");
		
		logDataItems2 = new ArrayList<String>();
		logDataItems2.add("Apr 21 21:47:10 localhost.localdomain haproxy[9898]: 192.65.41.20:4512 [21/Apr/2010:21:47:09.810] leads-http-fe leads-http-be/www1 0/0/97/131/319 200 390 - - ---- 38/38/34/34/0 0/0 {http://www.tek.com/products/oscilloscopes/dpo7000/|Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; .NET CLR 1.1.4322; .NET CLR 2.0.50727; .NET CLR 3.0.04506.648; .NET CLR 3.5.2|} \"GET /in.php?site_id=44619&type=ping&jsuid=5826455412892792485&mime=js&x=0.36977500072971475 HTTP/1.1\"\n");
		logDataItems2.add("Apr 21 21:47:10 localhost.localdomain haproxy[9898]: 127.0.0.1:35448 [21/Apr/2010:21:47:09.832] leads-http-fe leads-http-be/www1 4/0/98/104/207 200 390 - - ---- 36/36/32/32/0 0/0 {https://secure.gallerycollection.com/customizeorder/?step=5|Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; GTB0.0; .NET CLR 1.1.4322; .NET CLR 2.0.50727; .NET CLR 3.0.04506.30; .NET CL|72.54.165.186} \"GET /in.php?site_id=179651&res=1280x800&lang=en&secure=1&href=%2Fcustomizeorder%2F%3Fstep%3D5&title=The%20Gallery%20Collection&ref=&jsuid=1841384365995162438&mime=js&x=0.519074862698212 HTTP/1.1\"\n");
		logDataItems2.add("Apr 21 21:47:10 localhost.localdomain haproxy[9898]: 74.92.155.121:40160 [21/Apr/2010:21:47:09.859] leads-http-fe leads-http-be/www1 0/0/98/103/274 200 390 - - ---- 35/35/31/31/0 0/0 {http://www.search-autoparts.com/searchautoparts/Industry+News/Rejected-shops-line-up-to-fire-back-at-State-Farm/ArticleStandard/Article/detail/316550|Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; .NET CLR 1.1.4322; .NET CLR 2.0.50727; MS-RTC LM 8; .NET CLR 3.0.04506.6|} \"GET /in.php?site_id=78499&type=ping&jsuid=2197792080726781069&mime=js&x=0.5975263222122082 HTTP/1.1\"\n");
		logDataItems2.add("Apr 21 21:47:10 localhost.localdomain haproxy[9898]: 67.205.202.227:49917 [21/Apr/2010:21:47:09.891] leads-http-fe leads-http-be/www1 1/0/97/110/241 200 476 - - ---- 36/36/32/32/0 0/0 {http://www.hoovers.com/free/search/simple/xmillion/index.xhtml?which=company&query_string=ethanol+new+york&x=0&y=0|Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10.5; en-US; rv:1.9.0.19) Gecko/2010031218 Firefox/3.0.19|} \"GET /in.php?site_id=111664&res=1440x900&lang=en&secure=0&href=%2Ffree%2Fsearch%2Fsimple%2Fxmillion%2Findex.xhtml%3Fwhich%3Dcompany%26query_string%3Dethanol%2Bnew%2Byork%26x%3D0%26y%3D0&title=Company%20Search%20Results%20-%20Hoover%27s&ref=&jsuid=6694558525308952001&mime=js&x=0.4529488816381976 HTTP/1.1\"\n");
		

		final StringBuffer logData = new StringBuffer();
		for (String logEnt : logDataItems) { 
			logData.append(logEnt);
		}

		final StringBuffer logData2 = new StringBuffer();
		for (String logEnt : logDataItems2) { 
			logData2.append(logEnt);
		}

		logFileSources = new LogFileSource[] {
				new LogFileSource(UUID.randomUUID().toString(), parser) { 
					public Reader getReader() { 
						return new StringReader(logData.toString());
					}
				},
				new LogFileSource(UUID.randomUUID().toString(), parser) { 
					public Reader getReader() { 
						return new StringReader(logData2.toString());
					}
				}};
	}
	
	protected void tearDown() throws Exception {
		super.tearDown();
	}

	
	
	public void testCreateHttpRequestRunnerFromConfig() throws Exception {
		
		File tmp = File.createTempFile("test", ".log");
		tmp.deleteOnExit();
		Properties props = new Properties();
		props.setProperty("source", tmp.getCanonicalPath());
		props.setProperty("destination", "http://www.unittest.com");
		props.setProperty("numConcurrentRequestors", "2");
		props.setProperty("useInitialFiles", "true");
		props.setProperty("workQueueCapacity", "10");
		
		HAProxyLogReplayer replayer = HAProxyLogReplayer.createHttpRequestRunnerFromConfig(props);
		assertEquals(tmp.getCanonicalPath(), replayer.getSourceName());
		assertEquals("http://www.unittest.com", replayer.getDestination());
		assertEquals(2, replayer.getNumThreads());
		assertTrue(replayer.isUsingInitialFiles());
		assertEquals(10, replayer.getWorkQueueCapacity());
		
		props.setProperty("numConcurrentRequestors", "100");
		props.remove("useInitialFiles");

		replayer = HAProxyLogReplayer.createHttpRequestRunnerFromConfig(props);
		assertEquals(tmp.getCanonicalPath(), replayer.getSourceName());
		assertEquals("http://www.unittest.com", replayer.getDestination());
		assertEquals(100, replayer.getNumThreads());
		assertFalse(replayer.isUsingInitialFiles());
		assertEquals(10, replayer.getWorkQueueCapacity());
	}
	
	private HttpGetRequestRunner createTestReplayer() throws Exception { 
		File tmp = File.createTempFile("test", ".log");
		tmp.deleteOnExit();
		Properties props = new Properties();
		props.setProperty("source", tmp.getCanonicalPath());
		props.setProperty("destination", "http://www.unittest.com");
		props.setProperty("numConcurrentRequestors", "2");
		props.setProperty("useInitialFiles", "true");
		props.setProperty("workQueueCapacity", "10");
		
		return HAProxyLogReplayer.createHttpRequestRunnerFromConfig(props);
	}
	
	public void testCreateProducer() throws Exception { 
		HttpGetRequestRunner replayer = createTestReplayer();
		Producer<HttpGet> producer = replayer.createProducer();
		assertNotNull(producer);
		assertFalse(producer.isStarted());
		assertFalse(producer.isExhausted());
		assertFalse(producer.isFillInProgress());
		assertEquals(0, producer.getQueueDepth());
		LogFileSourceTracker tracker = producer.getLogFileSourceTracker();
		assertTrue(tracker instanceof RenamingStaticLogFileSourceTracker);
	}
	
	public void testCalculateInitialDeficitForThrottleWithOneZeroFiles() throws Exception {

		LogFileSourceIterator iter = new SequentialLogFileSourceIterator(System.getProperty("java.io.tmpdir"), parser, false) { 
			protected void initialize() { 
				// don't add any files
			}
		};
		
		long initialDeficit = HAProxyLogReplayer.calculateInitialDeficitForThrottle(iter, parser);
		assertEquals("no files = no deficit", 0, initialDeficit);
	}
	
	public void testCalculateInitialDeficitForThrottleWithOneInitialFile() throws Exception { 
		
		LogFileSourceIterator iter = new SequentialLogFileSourceIterator(System.getProperty("java.io.tmpdir"), parser, false) { 
			protected void initialize() { 
				// don't setup monitoring, instead pre-populate the queue with these String-based log file sources
				this.put(logFileSources[0]);
			}
		};
		
		assertEquals("expected number of sources were loaded into test fixture", 1, iter.toArray().length);
		
		long initialDeficit = HAProxyLogReplayer.calculateInitialDeficitForThrottle(iter, parser);
					
		assertEquals("one initial file (the one the logger is writing to now) = no deficit", 0, initialDeficit);
	}

	public void testCalculateInitialDeficitForThrottleWithMultipleInitialFiles() throws Exception { 
		
		LogFileSourceIterator iter = new SequentialLogFileSourceIterator(System.getProperty("java.io.tmpdir"), parser, false) { 
			protected void initialize() { 
				// don't setup monitoring, instead pre-populate the queue with these String-based log file sources
				for (LogFileSource lfs : logFileSources) { 
					this.put(lfs);
				}
			}
		};
		
		assertEquals("expected number of sources were loaded into test fixture", logFileSources.length, iter.toArray().length);
		
		long initialDeficit = HAProxyLogReplayer.calculateInitialDeficitForThrottle(iter, parser);
			
		String line0 = logDataItems.get(0); // first item from first file
		String lineN = logDataItems.get(logDataItems.size() - 1); // last item from last file
			
		HttpLogEntry entry0 = parser.parseLine(line0.replaceAll("\n", ""));
		HttpLogEntry entryN = parser.parseLine(lineN.replaceAll("\n", ""));
		
		assertEquals(entryN.getTimeMillis() - entry0.getTimeMillis(), initialDeficit);
	}
}
