package logreplay.producer;

import junit.framework.TestCase;
import logreplay.producer.TimeThrottle;
import logreplay.producer.TimeThrottle.ThrottleSchedule;


public class TimeThrottleTest extends TestCase {

	private long interval = 200;
	private long LOWER_PERCENTAGE_TOLERANCE = 80;
	private long UPPER_PERCENTAGE_TOLERANCE = 20;

	//Set of times representing timestamps from a log file with a one second granularity.
	//: 10:42:41
	//Therefore time1 can be used for say 3 events that occurred at 10:42:41, time2 for 5 events at 10:42:42, ...
	private long time1 = System.currentTimeMillis();
	private long time2 = time1 + 1000;
	private long time3 = time2 + 1000;

	public void testAheadOfSchedule() throws InterruptedException {
		TimeThrottle throttle = new TimeThrottle(interval);
		
		throttle.throttle(time1);
		assertThrottleResponse(throttle, 0 /* expected delay */, false /* behind schedule? */);
		
		throttle.throttle(time1);
		assertThrottleResponse(throttle, 0, false);
		
		throttle.throttle(time1);
		assertThrottleResponse(throttle, 0, false);

		
		throttle = new TimeThrottle(interval);
		
		//Expect time delay on number 4
		throttle.throttle(time2);
		assertThrottleResponse(throttle, time2 - time1 /* expected delay */, false /* behind schedule? */);
		
		throttle.throttle(time2);
		assertThrottleResponse(throttle, 0, false);

		throttle = new TimeThrottle(interval);
		//Expect time delay on number 6
		throttle.throttle(time3);
		assertThrottleResponse(throttle, time3 - time2, false);
		
		throttle.throttle(time3);
		assertThrottleResponse(throttle, 0, false);
	}

	public void testBehdindScehdule() throws Exception {
		TimeThrottle throttle = new TimeThrottle(interval);
		
		throttle.throttle(time1);
		assertThrottleResponse(throttle, 0, false);
		assertEquals(0, throttle.getLastThrottleDuration());
		Thread.sleep(100);

		throttle.throttle(time1);
		assertThrottleResponse(throttle, 0, false);
		assertEquals(0, throttle.getLastThrottleDuration());
		Thread.sleep(1110);

		//Would normally expect a time delay but we have not changed timestamps
		throttle.throttle(time1);
		assertThrottleResponse(throttle, 0, false);
		assertEquals(0, throttle.getLastThrottleDuration());

		//Change the timestamp and expect time delay as we have crossed a recheck interval
		throttle.throttle(time2);
		assertThrottleResponse(throttle, 0, true);
		assertTrue("Unexpected deficit encountered: " + throttle.getLastThrottleDuration(),
				throttle.getLastThrottleDuration() <= -210);

		throttle.throttle(time2);
		assertThrottleResponse(throttle, 0, true);
		assertTrue("Unexpected deficit encountered: " + throttle.getLastThrottleDuration(),
		        throttle.getLastThrottleDuration() <= -210);

		// currently we're 210 seconds behind schedule...
		// we log one 1000 ms in the future (790 ms ahead of schedule) so we should sleep for that time
		
		throttle.throttle(time3);
		assertThrottleResponse(throttle, 790, false);
		assertTrue("currently on schedule", throttle.getSchedule().getOffset() == 0);
		assertTrue("we slept for some amount of time", throttle.getLastThrottleDuration() > 0 && throttle.getLastThrottleDuration() <= 790);
		
		
		throttle.throttle(time3);
		assertThrottleResponse(throttle, 0, false);
		assertTrue("currently on schedule", throttle.getSchedule().getOffset() == 0);
		assertTrue("we slept for some amount of time", throttle.getLastThrottleDuration() > 0 && throttle.getLastThrottleDuration() <= 790);
	}
	
	public void testCalculatePauseTime() {
		ThrottleSchedule deficit = new ThrottleSchedule(100l);	// start w/100 deficit
		assertEquals(-90, deficit.adjustSchedule(10));			// last run was 10 faster than schedule, offset s/b -90
		assertEquals(-90, deficit.getOffset());					
		assertEquals(0, deficit.adjustSchedule(90));			// last run was another 90 faster than schedule, offset s/b 0 now
		assertEquals(0, deficit.getOffset());
	}

	public void testCalculatePauseTime_exceed() {
		ThrottleSchedule deficit = new ThrottleSchedule(100l); 	// start w/100 deficit
		assertEquals(10, deficit.adjustSchedule(110)); 			// last run was 110 faster than schedule, offset s/b +10
		assertEquals(10, deficit.getOffset());
		assertEquals(20, deficit.adjustSchedule(10)); 			// last run was another 10 faster than schedule, offset s/b +10
		assertEquals(20, deficit.getOffset());
	}

	public void testCalculatePauseTime_noDeficit() {
		ThrottleSchedule deficit = new ThrottleSchedule(0l); 	// start w/no deficit
		assertEquals(110, deficit.adjustSchedule(110));			// last run was 110 faster than schedule, offset s/b +110 
		assertEquals(110, deficit.getOffset());					
		assertEquals(200, deficit.adjustSchedule(90));			// last run was another 90 faster than schedule, offset s/b +200
		assertEquals(200, deficit.getOffset());
	}	

	// *********** TEST HELPER METHODS ************

	private void assertThrottleResponse(TimeThrottle throttle, long expectedDelay,
	        boolean behindSchedule) throws InterruptedException {

		ThrottleSchedule schedule = throttle.getSchedule();
		long scheduleOffset = schedule.getOffset();
		
		assertEquals("Throttle unexpectedly designated as behind schedule.", behindSchedule, scheduleOffset < 0);
		
		if (expectedDelay != 0) {
			Float percentage = (new Float(scheduleOffset) / new Float(expectedDelay)) * 100;
			//System.out.println("ThrottledDuration " + throttledDuration + " expectedDelay " + expectedDelay + " Percentage " + percentage);
			assertTrue("Throttle delay not within tolerance. ExpectedDelay: '" + expectedDelay + "', actualDelay: '"
			        + scheduleOffset + "'", (percentage > LOWER_PERCENTAGE_TOLERANCE)
			        || (percentage < UPPER_PERCENTAGE_TOLERANCE));
		} else {
			assertTrue("throttled duration = " + scheduleOffset + ", expected delay = " + expectedDelay, scheduleOffset <= 0);
		}
	}

}
