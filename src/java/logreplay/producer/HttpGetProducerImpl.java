/**
 * 
 */
package logreplay.producer;

import java.util.Iterator;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import logreplay.log.mapper.HttpLogEntryMapper;
import logreplay.log.source.LogFileSource;
import logreplay.log.tracker.LogFileSourceTracker;

import org.apache.http.client.methods.HttpGet;
import org.apache.log4j.Logger;


/**
 * Producer implementation can iterate over a web log and execute Http GET requests using apache commons http client.
 * @author cwoerner
 *
 * @param <T>
 */
public class HttpGetProducerImpl extends Producer<HttpGet> { 
	private static final Logger logger = Logger.getLogger(HttpGetProducerImpl.class);
	
	protected static final int DEFAULT_CAPACITY = 1000; 
	
	private final BlockingQueue<HttpGet> queue;
	private final int capacity;
	
	public HttpGetProducerImpl(HttpLogEntryMapper<HttpGet> mapper, Iterator<LogFileSource> sources, LogFileSourceTracker tracker) { 
		this(DEFAULT_CAPACITY, mapper, sources, tracker);  // TODO: change impl to avoid need for capacity
	}
	
	public HttpGetProducerImpl(int capacity, HttpLogEntryMapper<HttpGet> mapper, Iterator<LogFileSource> sources, LogFileSourceTracker tracker) { 
		super(sources, tracker, mapper);
		this.capacity = capacity;
    	this.queue = new ArrayBlockingQueue<HttpGet>(capacity);
	}
	
	@Override
	public HttpGet doTake() throws InterruptedException { 
		return queue.take();
	}


	@Override
	public long getQueueDepth() { 
		return queue.size();
	}

	@Override
	protected void put(HttpGet item) throws FillException {
		if (!queue.add(item)) { 
			throw new FillException("failed to enqueue item '" + item.toString() + "'");
		}
	}

	
	@Override
	protected void ensureCapacity() throws InterruptedException { 
		if (queue.size() == capacity) { 
			InterruptedException interrupted = null;
			boolean didLogWaitMsg = false;
			long startTime = System.currentTimeMillis();
			do { 
				try { 
					if (!didLogWaitMsg) { 
						logger.info("waiting for full queue to drain before enqueuing more data");
						didLogWaitMsg = true;
					}
					Thread.sleep(10);
				} catch (InterruptedException ie) { 
					logger.warn("thread interrupted while ensuring capacity", ie);
					interrupted = ie;
				}
			} while (queue.size() > (capacity / 2));
			
			logger.info("queue capacity reduced to " + queue.size() + ", continuing with fill; napped for " + (System.currentTimeMillis() - startTime) + " ms");
			
			if (null != interrupted) { 
				throw interrupted;
			}
		}
	}
	
}