package logreplay.log;

import java.io.File;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import logreplay.log.parser.LogFileSourceParser;
import logreplay.log.source.LogFileSource;
import logreplay.log.source.StaticLogFileSource;

import net.contentobjects.jnotify.JNotify;
import net.contentobjects.jnotify.JNotifyException;
import net.contentobjects.jnotify.JNotifyListener;

import org.apache.log4j.Logger;



public class SequentialLogFileSourceIterator extends LogFileSourceIterator {

	private static final Logger logger = Logger.getLogger(SequentialLogFileSourceIterator.class);	
	
	// \1 is prefix, \2 is sequence
	private static final Pattern LOG_FILE_PATTERN = Pattern.compile("^(clickstream_log_)(\\d{5})$");
	
	protected static class SequentialLogFileListener implements JNotifyListener {

		private final BlockingQueue<LogFileSource> sourceQueue;
		private final LogFileSourceParser parser;
		private Set<String> exclusionSet = new HashSet<String>();

		public SequentialLogFileListener(BlockingQueue<LogFileSource> sourceQueue, LogFileSourceParser parser) {
			this.sourceQueue = sourceQueue;
			this.parser = parser;
		}
		
		public SequentialLogFileListener(BlockingQueue<LogFileSource> sourceQueue, LogFileSourceParser parser, Set<String> exclusionSet) {
			this.sourceQueue = sourceQueue;
			this.parser = parser;
			this.exclusionSet = exclusionSet;
		}
		
		protected File getPreviousSequentialLogFile(String rootPath, String prefix, int curSequenceNum) { 
			int prevSequenceNum = curSequenceNum - 1;
			String prevFileName = String.format("%s%05d", prefix, prevSequenceNum);
			return new File(rootPath, prevFileName);
		}
		
		@Override
	    public void fileCreated(int wd, String rootPath, String name) {
			
			logger.info("handling file created event for fs obj '" + name + "'");
			
			try {
				
				if (exclusionSet.contains(new File(rootPath, name).getCanonicalPath())) {
					logger.info("File excluded: '" + name + "'");
					return;
				}

				
				Matcher matcher = LOG_FILE_PATTERN.matcher(name);
				if (matcher.matches()) {

					String prefix = matcher.group(1);
					String suffix = matcher.group(2);
					int sequence = Integer.parseInt(suffix);
						
					File logFile = getPreviousSequentialLogFile(rootPath, prefix, sequence);
					if (logFile.exists()) {
							
						logger.info("enqueueing log file source '" + logFile + "'");
						sourceQueue.put(new StaticLogFileSource(logFile, parser));
							
						int fileQueueDepth = sourceQueue.size();
						if (fileQueueDepth > 2) { 
							/* 
							 * queue depth will be 2 around boundary conditions where we 
							 * add a new file just as the previous file is being exhausted
							 */
							logger.warn("SequentialLogFileSourceIterator sources buffer is " + fileQueueDepth);
						}
					} else { 
						if (sequence == 0) { 
							logger.warn("ignoring initial sequence file until secondary file is created");
						} else { 
							logger.error("previous sequential logfile '" + logFile.getCanonicalPath() + "' does not exist");
						}
					}
				} else { 
					if (!name.equals("clickstream_log_current")) { 
						logger.warn("log file '" + name + "' doesn't match pattern");
					}
				}
				
			} catch (Exception e) {	
				logger.error("error creating logFile source for file '" + name + "'", e);
			}
        }
		public void fileRenamed(int wd, String rootPath, String oldName, String newName) {
	    }
	    public void fileModified(int wd, String rootPath, String name) {
	    }
	    public void fileDeleted(int wd, String rootPath, String name) {
	    }
    };

	
	
	private final LinkedBlockingQueue<LogFileSource> sources = new LinkedBlockingQueue<LogFileSource>();
	private final LogFileSourceParser parser;
	private final boolean useInitialFiles;
	private final Set<String> initialFiles = new HashSet<String>();
	private final String sourceDir;
	private int watchId;
	private boolean interrupted = false;
	
	/**
	 * throws NullPointerException if there is some problem setting up the notification infrastructure
	 */
	public SequentialLogFileSourceIterator(String sourceDir, LogFileSourceParser parser) {
		this(sourceDir, parser, false);
	}
	
	public SequentialLogFileSourceIterator(String sourceDir, LogFileSourceParser parser, boolean useInitialFiles) {
		this.sourceDir = sourceDir;
		this.parser = parser;
		this.useInitialFiles = useInitialFiles;
		initialize();
	}

	public Set<String> getInitialFiles() { 
		return initialFiles;
	}
	
	/**
	 * Whether there are any more sources.  Always returns true unless thread has been interrupted.
	 */
	@Override
	public synchronized boolean hasNext() {
		return !interrupted;
	}
	
	/**
	 * Take next source off the blocking queue.  If queue is empty, blocks until another file is ready.
	 * 
	 * @returns LogFileSource
	 */
	@Override
	public synchronized LogFileSource next() {
		LogFileSource source = null;
		try { 
			logger.info("retrieving next log file source");
			source = sources.take();
			logger.info("retrieved next log file source '" + source.getId() + "'. Size: " + size());
		} catch (InterruptedException ie) { 
			logger.error("thread interrupted while waiting on next log file source", ie);
			/*
			 *  reset interrupt status so that caller can handle.
			 *  Iterator api won't let us throw InterruptedException out of next() 
			 */
			Thread.currentThread().interrupt(); 
			interrupted = true;
		}

		return source;
	}
	
	public LogFileSource peek() { 
		return sources.peek();
	}
	

	public LogFileSource[] toArray() {
		return sources.toArray(new LogFileSource[] {});
	}
	
	/**
	 * Unsupported.
	 */
	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}
	
	/**
	 * Removes the watcher thread.
	 */
	public void shutdown() {
		try {
			logger.info("shutting down jnotify watch id  '" + watchId + "'");
			boolean res = JNotify.removeWatch(watchId);
			if (!res) {
				logger.error("remove watch returned false; maybe invalid watchId: " + watchId);
			}
			logger.info("removed jnotify watch id '" + watchId + "'");
		} catch (JNotifyException e) {
			logger.error("failed to cleanup jnotify watcher", e);
			throw new RuntimeException("failed to cleanup jnotify watcher", e);
		}
	}
		
	protected void initialize() { 
		try {
			if (useInitialFiles) {
				initialFiles.addAll(initializeWithExistingSources(sourceDir));
			}
			monitor(sourceDir, initialFiles);
		} catch (Throwable e) {
			throw (e instanceof RuntimeException) ? (RuntimeException)e : new RuntimeException(e);
		}
	}
	
	protected void put(LogFileSource lfs) {
		boolean putInterrupted = false;
		while (true) { 
			try { 
				this.sources.put(lfs);
				break;
			} catch (InterruptedException ie) { 
				logger.warn("thread interrupted during put", ie);
				/*
				 *  reset interrupt status so that caller can handle.
			 	*/
				putInterrupted = true;
			}
		}
		
		if (putInterrupted) { 
			interrupted = true;
			Thread.currentThread().interrupt(); 
		}
	}
	
	protected int size() { 
		return this.sources.size();
	}
	
	protected LogFileSourceParser getLogFileSourceParser() { 
		return this.parser;
	}
	
	protected boolean isUsingInitialFiles() { 
		return this.useInitialFiles;
	}

	protected static boolean isFileReplayable(String fileName) {
		Matcher matcher = LOG_FILE_PATTERN.matcher(fileName);
		return matcher.matches();
	}
	
	/**
	 * Initialize the sources queue with any already existing appropriate files in the sourceName directory
	 */
	protected synchronized Set<String> initializeWithExistingSources(String sourceDir) throws Exception {
		Set<String> exclusionSet = new HashSet<String>();
		try {
			File path = new File(sourceDir);
			File files[] = path.listFiles();
			
			/*
			 * Sort lexically to ensure sequential ordering of log files
			 */
			Arrays.sort(files, new Comparator<File>() {
				@Override
				public int compare(File arg0, File arg1) {
					return arg0.getName().compareTo(arg1.getName());
				}
			});
			
			logger.info("initializing from " + sourceDir + "; found " + files.length + " files");
			int counter = 0;
			for (File f : files) {
				String fileName = f.getName();
				if (isFileReplayable(fileName)) {
					counter++;
					logger.info("adding initial file " + f);
					sources.put(new StaticLogFileSource(f, parser));
					exclusionSet.add(f.getCanonicalPath());
				} 
			}
			logger.info("added " + counter + " files to queue: ");
		} catch (InterruptedException e) {
			logger.error("Unable to properly initialize iterator from existing contents of " + sourceDir, e);
			Thread.currentThread().interrupt();
			interrupted = true;
		}
		return exclusionSet;
	}
		
	/**
	 * Set up a watcher for create events to the sourceName directory, and when we get an appropriate
	 * addition, add it to the internal queue.
	 * 
	 * @param sourceName
	 */
	protected void monitor(String sourceName, Set<String> exclusionSet) throws Exception {
		if (null == sourceName) {
			throw new IllegalArgumentException("sourceName must not be null!");
		}

		watchId = JNotify.addWatch(sourceName, JNotify.FILE_CREATED, false, new SequentialLogFileListener(sources, parser, exclusionSet));
		    		    
		logger.info("directory watcher registered on '" + sourceName + "'");
    }

}