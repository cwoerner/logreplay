package logreplay;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import logreplay.log.HttpLogEntry;
import logreplay.log.LogFileSourceIterator;
import logreplay.log.SequentialLogFileSourceIterator;
import logreplay.log.mapper.HttpGetHttpLogEntryMapper;
import logreplay.log.mapper.HttpLogEntryMapper;
import logreplay.log.parser.LogFileSourceParser;
import logreplay.log.parser.haproxy.HAProxyLogParser;
import logreplay.log.source.LogFileSource;
import logreplay.log.source.StaticLogFileSource;
import logreplay.log.tracker.LogFileSourceTracker;
import logreplay.log.tracker.RenamingStaticLogFileSourceTracker;
import logreplay.producer.FillException;
import logreplay.producer.Producer;
import logreplay.producer.ThrottledHttpGetProducerImpl;
import logreplay.producer.TimeThrottle;

import org.apache.http.client.methods.HttpGet;
import org.apache.log4j.Logger;


/**
 * Replay customized haproxy log formatted data from sequentially named scribe logs.  
 * Renames the replayed files in the same directory after replaying them.
 *
 */
public class HAProxyLogReplayer extends HttpGetRequestRunner {

	private static final Logger logger = Logger.getLogger(HAProxyLogReplayer.class);
	
	private static final String propsFile = "haproxy-log-replay.properties";

	/**
	 * Creates a blocking queue and ThreadPool of HttpGetConsumer objects to pull from this queue.  
	 * Exits when all HttpGetConsumer object threads have finished and blocking queue is empty.
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		try { 
			final HttpGetRequestRunner runner = createHttpRequestRunnerFromConfig(getConfig(propsFile));
			
			final Thread execThread = Thread.currentThread();
			
			runner.registerShutdownHook(new Thread() {
				public void run() { 
					try { 
						execThread.interrupt();
					} finally { 
						runner.shutdown();
					}
				}
			});
			
			runner.run();
			
		} catch (IOException e) {
			logger.error("Unable to configure haproxy replay:", e);
        } 
	}	
	
	protected static HAProxyLogReplayer createHttpRequestRunnerFromConfig(Properties properties) {
		
	    String sourceName = System.getProperty("source");
	    if (null == sourceName) { 
	    	sourceName = properties.getProperty("source");
	    }
	    
	    String destination = System.getProperty("destination");
	    if (null == destination) { 
	    	destination = properties.getProperty("destination");
	    }
	    
	    String concurrentRequestors = System.getProperty("numConcurrentRequestors");
	    if (null == concurrentRequestors) { 
	    	concurrentRequestors = properties.getProperty("numConcurrentRequestors", "1");
	    }
	    int threads = Integer.parseInt(concurrentRequestors);
	    
	    
	    String initialFiles = System.getProperty("useInitialFiles");
	    if (null == initialFiles) { 
	    	initialFiles = properties.getProperty("useInitialFiles", "false");
	    }
	    boolean useInitialFiles = Boolean.parseBoolean(initialFiles);
	    
	    
	    String capacity = System.getProperty("workQueueCapacity");
	    if (null == capacity) { 
	    	capacity = properties.getProperty("workQueueCapacity", "1000");
	    }
	    int workQueueCapacity = Integer.parseInt(capacity);
	    
	    logger.info("{threads:" + threads + ", dest: " + destination + ", source:" + sourceName + ", initialFiles:" + useInitialFiles + "}");
	    
	    return new HAProxyLogReplayer(threads, sourceName, destination, useInitialFiles, workQueueCapacity);
    }
	
	private final boolean useInitialFiles;
	private final String sourceName;
	private final String destination;
	private final int workQueueCapacity;
	
	/**
	 * 
	 * @param numThreads
	 * @param sourceName a file path pointing to either an haproxy log file or a directory containing Scribe-managed haproxy log files with numerical sequence names.
	 * @param destination an http uri pointing to an endpoint capable of accepting requests replayed from the log file(s) from sourceName
	 * @param useInitialFiles whether or not to replay existing files in the directory pointed to by sourceName (only valid if sourceName is a directory)
	 */
	public HAProxyLogReplayer(int numThreads, String sourceName,
			String destination, boolean useInitialFiles, int workQueueCapacity) {
		super(numThreads);
		this.sourceName = sourceName;
		this.destination = destination;
		this.useInitialFiles = useInitialFiles;
		this.workQueueCapacity = workQueueCapacity;
	}

	public String getSourceName() { 
		return this.sourceName;
	}

	public String getDestination() { 
		return this.destination;
	}
	
	public boolean isUsingInitialFiles() { 
		return this.useInitialFiles;
	}
	
	public int getWorkQueueCapacity() { 
		return this.workQueueCapacity;
	}
	
	@Override
	protected Producer<HttpGet> createProducer() throws FillException { 

		/*
		 * Define producer dependencies, inject... if we refactor to mule/spring we'll just pull this into config.
		 */
		
		HAProxyLogParser parser = new HAProxyLogParser();
		HttpLogEntryMapper<HttpGet> entryMapper = new HttpGetHttpLogEntryMapper(getDestination());
		LogFileSourceTracker tracker = new RenamingStaticLogFileSourceTracker();
		File logSource = new File(getSourceName());

		Producer<HttpGet> producer;
		
		if (logSource.isDirectory()) {
			boolean runOldLogFiles = isUsingInitialFiles();
			
			final LogFileSourceIterator iterator = new SequentialLogFileSourceIterator(getSourceName(), parser, runOldLogFiles);
			
			if (runOldLogFiles) { 
				producer = new ThrottledHttpGetProducerImpl(workQueueCapacity, entryMapper, 
						iterator, tracker, 
						new TimeThrottle(calculateInitialDeficitForThrottle(iterator, parser)));

			} else { 
				producer = new ThrottledHttpGetProducerImpl(workQueueCapacity, entryMapper, 
						iterator, tracker);				
			}
			
		    this.registerShutdownHook(new Thread() {
		    	public void run() { 
		    		iterator.shutdown();
		    	}
		    });

		} else {
			List<LogFileSource> sources = new ArrayList<LogFileSource>();
			try { 
				sources.add(new StaticLogFileSource(logSource, parser));
			} catch (IOException ioe) { 
				throw new FillException("failed to create static log file " +
						"source from source named '" + getSourceName() + "'", ioe);
			}
			producer = new ThrottledHttpGetProducerImpl(workQueueCapacity, entryMapper, sources.iterator(), tracker);
		}
		
		return producer;
	}
	
	protected static long calculateInitialDeficitForThrottle(LogFileSourceIterator iterator, LogFileSourceParser parser) throws FillException { 

		long initialDeficit = 0;
		
		LogFileSource sources[] = iterator.toArray();
		
		if (sources.length <= 1) { 
			return initialDeficit;
		}
		
		LogFileSource oldestSource = sources[0];

		BufferedReader reader = null;
		try { 
			
			/* get the first log entry from the oldest historical file
			 * since we are only reading the first item we can't rely on the iterator
			 * to close the Reader for us.  That's why we call getReader() instead
			 * of getIterator() - so we can cleanup explicitly rather than having
			 * to iterate through everything in the iterator.
			 */
			reader = new BufferedReader(oldestSource.getReader());
			HttpLogEntry entry = parser.parseLine(reader.readLine());
			long firstTimeStamp = entry.getTimeMillis();
			reader.close();
			reader = null;

			// get the last log entry from the newest historical file
			LogFileSource newestSource = sources[sources.length - 2]; // may be same as oldest source
			logger.info("calculating last timestamp from latest historical source '" + newestSource.getId() + "'");
			

			reader = new BufferedReader(newestSource.getReader());
			Iterator<HttpLogEntry> logEntries = parser.parse(reader);
			HttpLogEntry tmpEntry = null;
			while (logEntries.hasNext()) { 
				// iterate through them all, saving the last not-null entry
				tmpEntry = logEntries.next();
				if (null != tmpEntry) 
					entry = tmpEntry;
			}
			
			long lastTimeStamp = entry.getTimeMillis();

			// the difference between the first entry in the oldest file and the last entry in the newest file 
			initialDeficit = lastTimeStamp - firstTimeStamp;
			
			if (initialDeficit < 0) { 
				logger.warn("initial deficit is < 0 (" + initialDeficit + ")");
				initialDeficit = 0;
			}
			
			logger.info("initial deficit is " + initialDeficit + " (" + firstTimeStamp + ", " + lastTimeStamp + ")");
			
		} catch (Exception e) { 
			throw new FillException("failed to synchronize throttle with oldest historical source '" + oldestSource.getId() + "'", e);
		} finally { 
			try { 
				if (null != reader) { 
					reader.close();
				}
			} catch (IOException ioe) { 
				throw new FillException(ioe);
			}
		}
		
		return initialDeficit;
	}

}
