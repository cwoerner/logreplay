package logreplay.log.tracker;

import logreplay.log.source.LogFileSource;

import org.apache.log4j.Logger;


/**
 * Simple implementation of a LogFileSourceTracker which just logs the fact that the processing took place.
 * @author cwoerner
 *
 */
public class LoggingLogFileSourceTracker implements LogFileSourceTracker {
	private final Logger logger = Logger.getLogger(LoggingLogFileSourceTracker.class);
	
	private String trackingStatus = null;
	
	public LoggingLogFileSourceTracker() {}
	
	/**
	 * Log the fact that the processed event took place.
	 */
	public boolean trackProcessed(LogFileSource source) throws InterruptedException {
		trackingStatus = "processed LogFileSource '" + source.getId() + "'";
		logger.info(trackingStatus);
		return true;
	}
	
	public String getTrackingStatus() { 
		return trackingStatus; 
	}
}
