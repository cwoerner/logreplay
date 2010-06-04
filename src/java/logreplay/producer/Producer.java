package logreplay.producer;

import java.io.IOException;
import java.text.ParseException;
import java.util.Iterator;
import java.util.concurrent.CountDownLatch;

import logreplay.log.HttpLogEntry;
import logreplay.log.mapper.HttpLogEntryMapper;
import logreplay.log.source.LogFileSource;
import logreplay.log.tracker.LogFileSourceTracker;

import org.apache.log4j.Logger;


/**
 * Fills a work queue for threads from one or more LogFileSources.
 * 
 * @author cwoerner
 *
 * @param <T>
 */
public abstract class Producer<T> {
	
	private final Logger logger = Logger.getLogger(Producer.class);

	private boolean isStarted = false;
	private boolean isInterrupted = false;
	private boolean fillInProgress = false;
	private final CountDownLatch barrier;
	private final Iterator<LogFileSource> sources;
	private final LogFileSourceTracker tracker;
	private final HttpLogEntryMapper<T> mapper;
	
	private final Object fillLock = new Object();

	public Producer(Iterator<LogFileSource> sources, LogFileSourceTracker tracker, HttpLogEntryMapper<T> mapper) {
		this.sources = sources;
    	this.tracker = tracker;
    	this.mapper = mapper;
    	this.barrier = new CountDownLatch(1);
	}

	
	/**
	 * Implement blocking take.
	 * @return
	 * @throws InterruptedException
	 */
	protected abstract T doTake() throws InterruptedException;
	
	/**
	 * Implement to put one item into the work queue.
	 * @param item
	 */
	protected abstract void put(T item) throws FillException;

	/**
	 * Implement to indicate the depth of a work queue
	 * @return
	 */
	public abstract long getQueueDepth();
	
	/**
	 * Dequeue an item from the internal work queue.  Block until Producer has started. Synchronized with isExhausted.
	 * @return
	 * @throws InterruptedException
	 */
	public T take() throws InterruptedException { 
		await();
		return doTake();
	}
	
	/**
	 * Indicates whether the current work queue is empty and there is no more data to fill the queue with.
	 * @return
	 */
	public boolean isExhausted() {
		return (isStarted() && !isFillInProgress() && (getQueueDepth() == 0) && !hasMoreSources());
	}

	public String getStateInfo() { 
		return "{isStarted=" + isStarted() + ", isFillInProgress=" + isFillInProgress() + ", queueDepth=" + getQueueDepth() + ", hasMoreSource=" + hasMoreSources() + "}";
	}
	
	/**
	 * Override to avoid buffer overflows, etc. 
	 */
	protected void ensureCapacity() throws InterruptedException {
		
	}
	
	/**
	 * Override to implement throttling, sampling, or filtering.
	 * @param httpReq
	 * @return
	 */
	protected boolean canAccept(HttpLogEntry entry) throws InterruptedException { 
		return true;
	}
	/**
	 * Returns the log file source tracker.
	 * @return
	 */
	public LogFileSourceTracker getLogFileSourceTracker() { 
		return this.tracker;
	}
	
	/**
	 * Indicates whether a file is currently being parsed, adding new data to a work queue.
	 * @return
	 */
	public boolean isFillInProgress() { 
		return this.fillInProgress;
	}
	
	/**
	 * Whether our iterator contains additional files we should process after completing the current (or first) file.
	 * @return
	 */
	private boolean hasMoreSources() { 
		return !testAndSetInterruptStatus() && sources.hasNext();
	}
	
	private LogFileSource nextSource() {
		return sources.next();
	}
	
	/**
	 * Whether we've been signaled as interrupted.
	 * @return
	 */
	protected boolean testAndSetInterruptStatus() {
		if (Thread.interrupted()) { 
			this.isInterrupted = true;
			Thread.currentThread().interrupt();
		}
		
		return this.isInterrupted;
	}
	
	
	/**
	 * Fills the queue from the next LogFileSource in the sources iterator.
	 * @throws FillException
	 */
	protected void fillQueue() throws FillException {
		synchronized (fillLock) { 
			logger.info("Filling queue...");
			
			if (!sources.hasNext()) {
				throw new FillException("no sources from which to fill work queue");
			}
			
			LogFileSource source = null;
			while (hasMoreSources()) { 
				try { 
					fillInProgress = true; // must be set before source.next()
					source = nextSource();
					if (source.exists()) { 
						fillFromSource(source); 
						tracker.trackProcessed(source);
					} else { 
						logger.warn("ignoring non-existant source '" + source.getId() + "'");
					}
				} catch (InterruptedException ie) {
					logger.warn("interrupted during fill", ie);
					isInterrupted = true;
				} finally { 
					fillInProgress = false;
					testAndSetInterruptStatus();
				}
			}
			
			logger.info("fill complete");
			
			if (isInterrupted) {
				logger.warn("interrupted during fill, terminating log file source iteration");
			}
		}
	}
	
	/**
	 * Fill a work queue from the LogFileSource source.
	 * @param source
	 * @throws FillException
	 */
	protected void fillFromSource(LogFileSource source) throws FillException { 
		Iterator<HttpLogEntry> httpReqInfos = parseLogFileSource(source);
		String srcId = source.getId();
		logger.info("filling from source '" + srcId + "'");
		
		long reportFreq = 100, i = 0, mult = 1;
		T mappedObj = null;
		boolean itemHandled = false;
		
		if (testAndSetInterruptStatus()) { 
			logger.info("aborting fill from source due to interruption");
			return;
		}
		
		while (httpReqInfos.hasNext()) {
			if ((i > 0) && ((i % reportFreq) == 0)) {
				logger.info("filling item # " + (i * mult) + " from source '" + srcId + "'");
			} 
			i = i >= Long.MAX_VALUE ? 0 : i + 1;
			
			HttpLogEntry httpReq = httpReqInfos.next();
			
			if (null != httpReq) {
				itemHandled = false;
				while (!itemHandled) { 
					try { 
						if (canAccept(httpReq)) {
							ensureCapacity();
							mappedObj = mapper.map(httpReq);
							if (null != mappedObj) { 
								put(mappedObj);
							}
						}
						itemHandled = true;
					} catch (InterruptedException ie) { 
						logger.warn("interrupted filling from source", ie);
						isInterrupted = true;
					}
				}
			} else { 
				logger.warn("rejecting entry");
			}
		}
		
		logger.info("filled " + (i * mult) + " items from source '" + srcId + "'");
	}
	
	/**
	 * Called to accept a new LogFileSource
	 * @param source
	 * @return
	 * @throws FillException
	 */
	protected Iterator<HttpLogEntry> parseLogFileSource(LogFileSource source) throws FillException {
		
		if (null == source) { 
			throw new FillException("null source encountered");
		}
		
		Iterator<HttpLogEntry> httpReqIter;
		try { 
			httpReqIter = source.getIterator();
		} catch (IOException fnfe) { 
			throw new FillException("failed to read haproxy log", fnfe);
		} catch (ParseException pe) { 
			throw new FillException("failed to parse haproxy log", pe);
		}
		
		return httpReqIter;
	}

	/**
	 * Blocks until this Producer has started filling the work queue.
	 * @throws InterruptedException
	 */
	public void await() throws InterruptedException {
		if (isStarted()) { 
			return;
		}
		
		barrier.await();
	}
	
	/**
	 * Start filling the queue and notify any consumer blocking on await().
	 * @throws FillException
	 */
	public void start() throws FillException { 
		if (isStarted()) {
			return;
		}

		isStarted = true;

	    barrier.countDown();
	    
	    if (Thread.interrupted()) { 
	    	logger.info("aborting start due to interrupt");
	    	isInterrupted = true;
	    	Thread.currentThread().interrupt();
	    	return;
	    }
	    
		fillQueue();
	}
	
	/**
	 * Indicates that this Producer has started filling a work queue.
	 * @return
	 */
	public boolean isStarted() {
		return isStarted;
	}
	
}