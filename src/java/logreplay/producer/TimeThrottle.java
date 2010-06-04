package logreplay.producer;

import org.apache.log4j.Logger;

public class TimeThrottle {
	
	protected static class ThrottleSchedule {

		private long millisBehindScheduleSinceStart = 0l;
		
		public ThrottleSchedule(long millisBehind) {
			this.millisBehindScheduleSinceStart = millisBehind;
		}
		
		public long adjustSchedule(long millisAheadOfScheduleThisPeriod) {
			millisBehindScheduleSinceStart = millisBehindScheduleSinceStart - millisAheadOfScheduleThisPeriod;
			return getOffset(); // return how far off schedule we are right now
		}
		
		public void reset() { 
			millisBehindScheduleSinceStart = 0;
		}

		/**
		 * < 0 behind schedule
		 * > 0 ahead of schedule, called should sleep 
		 * @return
		 */
		public long getOffset() {
			return -millisBehindScheduleSinceStart;
		}
	}

	private static final Logger logger = Logger.getLogger(TimeThrottle.class);
	
	private final long throttlePeriodDuration;
	private final ThrottleSchedule throttleSchedule;
	private long periodStartTime;
	private long periodLogStartTime = 0;
	private long lastThrottleDuration = 0;

	public TimeThrottle(long throttleFrequencyMillis, long initialDeficit) { 
		this.throttlePeriodDuration = throttleFrequencyMillis;
		this.throttleSchedule = new ThrottleSchedule(initialDeficit);	
	}
	
	public TimeThrottle(long throttleFrequencyMillis) {		
		this(throttleFrequencyMillis, 0);
	}

	public ThrottleSchedule getSchedule() { 
		return this.throttleSchedule;
	}
	
	/**
	 * Modifies ThrottleSchedule.
	 * @param logTime
	 * @param throttleSchedule
	 */
	public void throttle(long logTime) throws InterruptedException {

		if (periodLogStartTime == 0) { 
			// first entry, set baseline times
			startNewPeriod(logTime);
		}
		
		long periodLogElapsedTime = logTime - periodLogStartTime;	
		if (periodLogElapsedTime >= throttlePeriodDuration) {
			
			/*
			 *  enough time has passed between log time stamps that we 
			 *  must check whether we're on throttleSchedule again
			 */

			long periodElapsedTime = System.currentTimeMillis() - periodStartTime;

			long millisAheadOfSchedule = periodLogElapsedTime - periodElapsedTime;
			
			try {
				lastThrottleDuration = throttleSchedule.adjustSchedule(millisAheadOfSchedule);
				if (lastThrottleDuration > 0) { 
					Thread.sleep(lastThrottleDuration);
					// now we slept to get back on track, so adjust the schedule
					throttleSchedule.reset();
					if (logger.isDebugEnabled()) { 
						logger.debug("Slept for " + lastThrottleDuration + " millis.");
					}
				}
				
			} finally { 
				// every throttlePeriodDuration millis elapsed completes another period
				startNewPeriod(logTime);
			}
		}
	}
	
	protected long getLastThrottleDuration() { 
		return lastThrottleDuration;
	}
	
	private void startNewPeriod(long logStartTime) { 
		periodStartTime = System.currentTimeMillis();
		periodLogStartTime = logStartTime;
	}
}
