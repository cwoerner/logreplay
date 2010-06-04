package logreplay.log.tracker;

import logreplay.log.source.LogFileSource;

public interface LogFileSourceTracker {

	/**
	 * Implements some tracking functionality and returns whether the tracking functionality was completed.
	 * @param source
	 * @return
	 */
	public boolean trackProcessed(LogFileSource source) throws InterruptedException;

	public String getTrackingStatus();
}