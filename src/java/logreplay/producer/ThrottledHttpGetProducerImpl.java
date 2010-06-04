package logreplay.producer;

import java.text.ParseException;
import java.util.Iterator;

import logreplay.log.HttpLogEntry;
import logreplay.log.mapper.HttpLogEntryMapper;
import logreplay.log.source.LogFileSource;
import logreplay.log.tracker.LogFileSourceTracker;

import org.apache.http.client.methods.HttpGet;
import org.apache.log4j.Logger;


public class ThrottledHttpGetProducerImpl extends HttpGetProducerImpl {

	private static final Logger logger = Logger.getLogger(ThrottledHttpGetProducerImpl.class);

	private TimeThrottle throttle = null;
	private static final long DEFAULT_THROTTLE_FREQUENCY = 1000; // no point in making this less granularity than the log timestamps (second)
	
	public ThrottledHttpGetProducerImpl(HttpLogEntryMapper<HttpGet> mapper,
			Iterator<LogFileSource> sources, LogFileSourceTracker tracker) {
		this(DEFAULT_CAPACITY, mapper, sources, tracker);
	}

	public ThrottledHttpGetProducerImpl(int capacity,
			HttpLogEntryMapper<HttpGet> mapper, Iterator<LogFileSource> sources, LogFileSourceTracker tracker) {
		this(capacity, mapper, sources, tracker, new TimeThrottle(DEFAULT_THROTTLE_FREQUENCY));
	}
	
	public ThrottledHttpGetProducerImpl(int capacity,
			HttpLogEntryMapper<HttpGet> mapper, Iterator<LogFileSource> sources, LogFileSourceTracker tracker, TimeThrottle throttle) {
		super(capacity, mapper, sources, tracker);
		this.throttle = throttle;
	}
	
	@Override
	protected boolean canAccept(HttpLogEntry httpReq) throws InterruptedException { 
		
		try {
			throttle.throttle(httpReq.getTimeMillis());
		} catch (ParseException e) {
			logger.error("Unparsable time encountered", e);
		}

		return true;
	}
}
