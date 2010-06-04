package logreplay.producer;

import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.BrokenBarrierException;

import junit.framework.TestCase;
import logreplay.consumer.HttpGetConsumer;
import logreplay.consumer.Consumer.ExitStatus;
import logreplay.log.mapper.HttpGetHttpLogEntryMapper;
import logreplay.log.parser.haproxy.HAProxyLogParser;
import logreplay.log.source.LogFileSource;
import logreplay.log.tracker.LoggingLogFileSourceTracker;
import logreplay.producer.FillException;
import logreplay.producer.HttpGetProducerImpl;
import logreplay.producer.Producer;

import org.apache.http.client.methods.HttpGet;


public class HttpGetProducerImplTest extends TestCase {

	private HttpGetProducerImpl producer;
	private List<String> logDataItems;
	
	protected void setUp() throws Exception {
		super.setUp();
		
		
		logDataItems = new ArrayList<String>();
		logDataItems.add("Apr 21 21:47:10 localhost.localdomain haproxy[9898]: 204.29.15.2:26455 [21/Apr/2010:21:47:09.799] leads-http-fe leads-http-be/www1 0/0/99/105/239 200 390 - - ---- 37/37/32/32/0 0/0 {http://www.demandbase.com/directory/davis_street_land_co_of_tennessee_llc-tn-nashville-6519-business-contacts|Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; GTB0.0; .NET CLR 1.1.4322; .NET CLR 2.0.50727; MS-RTC LM 8; InfoPath.2; .NET |} \"GET /in.php?site_id=32783&res=1600x1200&lang=en&secure=0&href=%2Fdirectory%2Fdavis_street_land_co_of_tennessee_llc-tn-nashville-6519-business-contacts&title=Davis%20Street%20Land%20Co%20Of%20Tennessee%20Llc%2C%20Nashville%20TN%20business%20contact%20information%20and%20company%20information%20from%20Demandbase&ref=&jsuid=3376167984378186865&mime=js&x=0.7273904391287356 HTTP/1.1\"\n");
		logDataItems.add("Apr 21 21:47:10 localhost.localdomain haproxy[9898]: 127.0.0.1:35448 [21/Apr/2010:21:47:09.832] leads-http-fe leads-http-be/www1 4/0/98/104/207 200 390 - - ---- 36/36/32/32/0 0/0 {https://secure.gallerycollection.com/customizeorder/?step=5|Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; GTB0.0; .NET CLR 1.1.4322; .NET CLR 2.0.50727; .NET CLR 3.0.04506.30; .NET CL|72.54.165.186} \"GET /in.php?site_id=179651&res=1280x800&lang=en&secure=1&href=%2Fcustomizeorder%2F%3Fstep%3D5&title=The%20Gallery%20Collection&ref=&jsuid=1841384365995162438&mime=js&x=0.519074862698212 HTTP/1.1\"\n");
		logDataItems.add("Apr 21 21:47:10 localhost.localdomain haproxy[9898]: 161.231.132.14:2263 [21/Apr/2010:21:47:09.646] leads-http-fe leads-http-be/www1 0/0/97/115/399 200 390 - - ---- 36/36/31/31/0 0/0 {http://www.zetta.net/aboutUs.php|Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.9.2.3) Gecko/20100401 Firefox/3.6.3|} \"GET /in.php?site_id=98373&type=ping&jsuid=3541741274080471221&mime=js&x=0.7638588032561825 HTTP/1.1\"\n");
		logDataItems.add("Apr 21 21:47:10 localhost.localdomain haproxy[9898]: 206.248.45.211:51551 [21/Apr/2010:21:47:09.799] leads-http-fe leads-http-be/www1 0/0/99/106/255 200 390 - - CD-- 37/37/32/32/0 0/0 {http://www.informationweek.com/global-cio/index.jhtml;jsessionid=W5UTVENKKWHKVQE1GHPSKHWATMY32JVN?cid=iwk-header-navbar-globalcio|Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; Trident/4.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.3072|} \"GET /in.php?site_id=152572&type=ping&jsuid=5449293091384845075&mime=js&x=0.7651688018478069 HTTP/1.1\"\n");
		logDataItems.add("Apr 21 21:47:10 localhost.localdomain haproxy[9898]: 68.13.225.13:63585 [21/Apr/2010:21:47:09.779] leads-http-fe leads-http-be/www1 20/0/99/99/283 200 390 - - ---- 36/36/32/32/0 0/0 {http://www.informationweek.com/news/windows/showArticle.jhtml?articleID=189400897&pgno=2&queryText=&isPrev=|Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US) AppleWebKit/533.2 (KHTML, like Gecko) Chrome/5.0.342.9 Safari/533.2|} \"GET /in.php?site_id=152572&type=ping&jsuid=9287717478008707382&mime=js&x=0.8806299325078726 HTTP/1.1\"\n");
		logDataItems.add("Apr 21 21:47:10 localhost.localdomain haproxy[9898]: 71.98.63.188:28738 [21/Apr/2010:21:47:09.513] leads-http-fe leads-http-be/www1 18/0/99/101/592 200 476 - - ---- 38/38/35/35/0 0/0 {http://www.marketingexperiments.com/|Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 5.1; Trident/4.0; GTB6.4; .NET CLR 1.1.4322)|} \"GET /in.php?site_id=103061&res=1440x900&lang=en&secure=0&href=%2F&title=Discover%20Which%20Marketing%20Programs%20Really%20Work&ref=&jsuid=2560584024606250453&mime=js&x=0.5277149046036997 HTTP/1.1\"\n");
		logDataItems.add("Apr 21 21:47:10 localhost.localdomain haproxy[9898]: 192.65.41.20:4512 [21/Apr/2010:21:47:09.810] leads-http-fe leads-http-be/www1 0/0/97/131/319 200 390 - - ---- 38/38/34/34/0 0/0 {http://www.tek.com/products/oscilloscopes/dpo7000/|Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; .NET CLR 1.1.4322; .NET CLR 2.0.50727; .NET CLR 3.0.04506.648; .NET CLR 3.5.2|} \"GET /in.php?site_id=44619&type=ping&jsuid=5826455412892792485&mime=js&x=0.36977500072971475 HTTP/1.1\"\n");
		logDataItems.add("Apr 21 21:47:10 localhost.localdomain haproxy[9898]: 99.160.163.179:55591 [21/Apr/2010:21:47:09.815] leads-http-fe leads-http-be/www1 3/0/97/123/317 200 390 - - ---- 37/37/33/33/0 0/0 {http://www.climber.com/recruiter_lists|Mozilla/5.0 (Windows; U; Windows NT 6.0; en-US) AppleWebKit/532.5 (KHTML, like Gecko) Chrome/4.1.249.1045 Safari/532.5|} \"GET /in.php?site_id=60288&type=ping&jsuid=4961960877218468906&mime=js&x=0.042675537057220936 HTTP/1.1\"\n");
		logDataItems.add("Apr 21 21:47:10 localhost.localdomain haproxy[9898]: 67.205.202.227:49917 [21/Apr/2010:21:47:09.891] leads-http-fe leads-http-be/www1 1/0/97/110/241 200 476 - - ---- 36/36/32/32/0 0/0 {http://www.hoovers.com/free/search/simple/xmillion/index.xhtml?which=company&query_string=ethanol+new+york&x=0&y=0|Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10.5; en-US; rv:1.9.0.19) Gecko/2010031218 Firefox/3.0.19|} \"GET /in.php?site_id=111664&res=1440x900&lang=en&secure=0&href=%2Ffree%2Fsearch%2Fsimple%2Fxmillion%2Findex.xhtml%3Fwhich%3Dcompany%26query_string%3Dethanol%2Bnew%2Byork%26x%3D0%26y%3D0&title=Company%20Search%20Results%20-%20Hoover%27s&ref=&jsuid=6694558525308952001&mime=js&x=0.4529488816381976 HTTP/1.1\"\n");
		logDataItems.add("Apr 21 21:47:10 localhost.localdomain haproxy[9898]: 74.92.155.121:40160 [21/Apr/2010:21:47:09.859] leads-http-fe leads-http-be/www1 0/0/98/103/274 200 390 - - ---- 35/35/31/31/0 0/0 {http://www.search-autoparts.com/searchautoparts/Industry+News/Rejected-shops-line-up-to-fire-back-at-State-Farm/ArticleStandard/Article/detail/316550|Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; .NET CLR 1.1.4322; .NET CLR 2.0.50727; MS-RTC LM 8; .NET CLR 3.0.04506.6|} \"GET /in.php?site_id=78499&type=ping&jsuid=2197792080726781069&mime=js&x=0.5975263222122082 HTTP/1.1\"\n");
		

		final StringBuffer logData = new StringBuffer();
		for (String logEnt : logDataItems) { 
			logData.append(logEnt);
		}
		
		producer = new HttpGetProducerImpl(
				new HttpGetHttpLogEntryMapper("http://localhost"), 
				(Iterator<LogFileSource>)Arrays.asList(new LogFileSource[] {
						new LogFileSource(UUID.randomUUID().toString(), new HAProxyLogParser()) { 
							public Reader getReader() { 
								return new StringReader(logData.toString());
							}
						}
				}).iterator(), 
				new LoggingLogFileSourceTracker());
	}

	protected void tearDown() throws Exception {
		super.tearDown();
	}

	public void testGetQueueDepth() throws FillException {
		assertEquals(0, producer.getQueueDepth());
		producer.start();
		assertEquals(logDataItems.size(), producer.getQueueDepth());
	}


	public void testProducerWithConsumers() throws FillException {
		
		final class NonExecutingHttpGetConsumer extends HttpGetConsumer {

			private int numExecs = 0;
			
			public NonExecutingHttpGetConsumer(Producer<HttpGet> producer) { 
				super(producer);
			}

			@Override
			public long execute(HttpGet get) {
				numExecs++;
				return 1L;
			}
			
			public int getNumExecs() { 
				return numExecs;
			}
		};
		
		// create 3 consumer threads, should await on barrier until producer starts
		NonExecutingHttpGetConsumer con1 = new NonExecutingHttpGetConsumer(producer);
		NonExecutingHttpGetConsumer con2 = new NonExecutingHttpGetConsumer(producer);
		NonExecutingHttpGetConsumer con3 = new NonExecutingHttpGetConsumer(producer);
		
		Thread c1 = new Thread(con1);
		Thread c2 = new Thread(con2);
		Thread c3 = new Thread(con3);

		c1.start();
		c2.start();
		c3.start();
		
		assertEquals(0, producer.getQueueDepth());

		producer.start(); // count down the barrier and threads start take()'ing work off the queue

		try { 
			Thread.sleep(1000);
		} catch (InterruptedException ie) { 
			ie.printStackTrace();
		}
		
		/* 
		 * consumers can block on a final take(), so it's the 
		 * responsibility of the top-level system to interrupt 
		 * the consumer threads.  It's better to do this than to
		 * introduce some sort of producer/consumer synchronization
		 * on every queue interaction just to void having to 
		 * interrupt consumers at shutdown.
		 */
		c1.interrupt();
		c2.interrupt();
		c3.interrupt();
		
		while (true) {
			try { 
				c1.join();
				c2.join();
				c3.join();
				break;
			} catch (InterruptedException ie) { 
				ie.printStackTrace();
			}
		}
		
		assertEquals("all items in queue were consumed", 0, producer.getQueueDepth());
		assertEquals("all items consumed were executed", logDataItems.size(), con1.getNumExecs() + con2.getNumExecs() + con3.getNumExecs());
		
		assertTrue("exit status should be interrupted or ok", ExitStatus.error != con1.getExitStatus());
		assertTrue("exit status should be interrupted or ok", ExitStatus.error != con2.getExitStatus());
		assertTrue("exit status should be interrupted or ok", ExitStatus.error != con3.getExitStatus());
	}

	
	public void testTake() throws BrokenBarrierException, FillException {
		HttpGetConsumer consumer = new HttpGetConsumer(producer) {
			private int iters = 0;
			@Override
			protected boolean hasNext() {
				// force first iter true to enter blocking take so we can interrupt
				return iters++ == 0 ? true : super.hasNext();
			}
			@Override
			public long execute(HttpGet get) { 
				return 1L;
			}
		};
		Thread c1 = new Thread(consumer);

		// fill queue before starting consumer thread
		producer.start(); 

		assertEquals(logDataItems.size(), producer.getQueueDepth());
		
		// take everything off the queue
		int i = 0;
		while (!producer.isExhausted()) {
			try { 
				assertNotNull(producer.take());
			} catch (InterruptedException ie) { 
				ie.printStackTrace();
			}
			i++;
		}
		assertEquals("make sure we actually took everything", logDataItems.size(), i);
		
		// start the consumer, should be nothing there to take, so it will block
		c1.start();
		
		
		// bah, sleep, giving c1 enough time to get into blocking take()
		try { 
			Thread.sleep(1000);
		} catch (InterruptedException ie) { 
			ie.printStackTrace();
		}
		
		// interrupt thread during blocking take(), join
		c1.interrupt();
		
		while (true) {
			try { 
				c1.join();
				break;
			} catch (InterruptedException ie) { 
				ie.printStackTrace();
			}
		} 
		
		assertEquals(ExitStatus.interrupted, consumer.getExitStatus());
	}
	
}
