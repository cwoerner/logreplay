package logreplay.log.tracker;

import java.io.File;
import java.io.IOException;

import logreplay.log.source.LogFileSource;
import logreplay.log.source.StaticLogFileSource;

import org.apache.log4j.Logger;


/**
 * Renames the underlying File for a StaticLogFileSource after it's been processed. 
 * @author cwoerner
 *
 */
public class RenamingStaticLogFileSourceTracker implements LogFileSourceTracker {
	public static final String SUFFIX = ".replayed";

	private static final Logger logger = Logger.getLogger(RenamingStaticLogFileSourceTracker.class);

	private String trackingStatus = null;
	
	public RenamingStaticLogFileSourceTracker() { 
	}
	
	protected File makeTargetFileForRename(File logFile) throws IOException { 
		return new File(logFile.getCanonicalPath() + SUFFIX + "." + System.currentTimeMillis());
	}
	
	/**
	 * LogFileSource must be an instance of StaticLogFileSource, otherwise it's
	 * a no-op.
	 * 
	 * Renames the underlying File from the StaticLogFileSource with a 
	 * .replayed.<currentTimeMillis> extension.  Continues sleeping 10ms and 
	 * generating new time-stamped names until a suitable filename has been 
	 * identified. 
	 */
	public boolean trackProcessed(LogFileSource source) throws InterruptedException {
		if (!(source instanceof StaticLogFileSource)) { 
			logger.warn("ignoring rename on LogFileSource of type " + source + "");
			trackingStatus = "ignored rename on LogFileSource of type " + source;
			return false;
		}
		
		boolean renamed = false;
		
		StaticLogFileSource fileSource = (StaticLogFileSource)source;
		File logFile = fileSource.getFile();
		File newLogFileName = null;
		InterruptedException interrupted = null;

		try {
			newLogFileName = makeTargetFileForRename(logFile);
			
			while (newLogFileName.exists()) { 
				logger.warn("log file '" + newLogFileName + "' exists, renaming");
				try { 
					Thread.sleep(10); 
				} catch (InterruptedException ie) { 
					logger.warn("thread interrupted while sleeping on rename", ie);
					interrupted = ie;
				}
				newLogFileName = makeTargetFileForRename(logFile);
			}
			
			logger.info("renaming log file '" + logFile + "' -> '" + newLogFileName + "'");
			
			renamed = logFile.renameTo(newLogFileName);

		} catch (IOException ioe) { 
			logger.error(ioe);
		} finally { 
			if (null != interrupted) { 
				throw interrupted;
			}
		}
		
		if (renamed) { 
			trackingStatus = "renamed log file '" + logFile + "' -> '" + newLogFileName + "'";
			return true;
		} else { 
			logger.error("failed to rename log file '" + logFile + "'");
			return false;
		}
	}
	
	/**
	 * Returns a string describing the last rename.  Not threadsafe.
	 */
	public String getTrackingStatus() { 
		return trackingStatus;
	}
}
