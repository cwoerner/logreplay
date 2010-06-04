package logreplay.log.tracker;

import java.io.File;
import java.io.Reader;
import java.io.StringReader;
import java.util.UUID;


import junit.framework.TestCase;
import logreplay.log.parser.haproxy.HAProxyLogParser;
import logreplay.log.source.LogFileSource;
import logreplay.log.source.StaticLogFileSource;
import logreplay.log.tracker.RenamingStaticLogFileSourceTracker;

public class RenamingStaticLogFileSourceTrackerTest extends TestCase {

	private RenamingStaticLogFileSourceTracker tracker;
	protected void setUp() throws Exception {
		super.setUp();
		tracker = new RenamingStaticLogFileSourceTracker();
	}

	protected void tearDown() throws Exception {
		super.tearDown();
	}

	public void testTrackProcessedNonStaticLogFileSourceReturnsFalse() throws InterruptedException { 
		assertFalse(tracker.trackProcessed(new LogFileSource(UUID.randomUUID().toString(), new HAProxyLogParser()) { 
			public Reader getReader() { 
				return new StringReader("");
			}
		}));
	}
	
	public void testTrackProcessed() throws Exception { 
		File tmp = File.createTempFile("unittest", ".txt");
		String expectedStatus = "renamed log file '" + tmp.getCanonicalPath() + "' -> '" + tmp.getCanonicalPath() + ".replayed";
		tmp.deleteOnExit();
		assertTrue(tracker.trackProcessed(new StaticLogFileSource(tmp, new HAProxyLogParser())));
		assertFalse(tmp.exists());
		assertTrue(tracker.getTrackingStatus(), tracker.getTrackingStatus().startsWith(expectedStatus));
	}
	
	public void testMakeTargetFileForRename() throws Exception { 
		File newFileName = tracker.makeTargetFileForRename(new File("test"));
		assertTrue(newFileName.getName().startsWith("test.replayed."));
	}
}
