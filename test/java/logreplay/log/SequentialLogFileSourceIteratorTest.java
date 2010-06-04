package logreplay.log;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import junit.framework.TestCase;
import logreplay.log.SequentialLogFileSourceIterator;
import logreplay.log.SequentialLogFileSourceIterator.SequentialLogFileListener;
import logreplay.log.parser.haproxy.HAProxyLogParser;
import logreplay.log.source.LogFileSource;


public class SequentialLogFileSourceIteratorTest extends TestCase {
	private static final String prefix = "clickstream_log_";
	private static final String suffix1 = "00000";
	private static final String suffix2 = "00001";

	private String dir;

	private TestFileGenerator t = null;
	private NewFileGenerator t2 = null;
	
	protected void setUp() throws Exception {
		super.setUp();
		dir = System.getProperty("java.io.tmpdir");
		t = new TestFileGenerator();
		t2 = new NewFileGenerator();
	}


	protected void tearDown() throws Exception {
		super.tearDown();
		if (null != t) t.deleteAllFiles();
		if (null != t2) t2.deleteAllFiles();
	}
	
	
	public void testNextWithLiveWatcher() throws IOException {

		// create the test file first; it needs to be in place before we set up the iterator to wait for the 2nd file

		t.start();
		SequentialLogFileSourceIterator it = new SequentialLogFileSourceIterator(dir, new HAProxyLogParser());
			
		// now we can create the new file, which will trigger use of the test file

		t2.start();
		
	    if (it.hasNext()) {
	    	LogFileSource name = it.next();
	    	assertEquals(name.getId(), new File(new File(dir), prefix + suffix1).getCanonicalPath());
	    }
	}
	
	public void testIsLogfileReplayable() { 
		assertTrue(SequentialLogFileSourceIterator.isFileReplayable("clickstream_log_12345"));
		
		assertFalse(SequentialLogFileSourceIterator.isFileReplayable("clickstream_log_12345x"));
		assertFalse(SequentialLogFileSourceIterator.isFileReplayable("clickstream_log_x12345"));
		assertFalse(SequentialLogFileSourceIterator.isFileReplayable("clickstream_logx_12345"));
		assertFalse(SequentialLogFileSourceIterator.isFileReplayable("xclickstream_log_12345"));
		assertFalse(SequentialLogFileSourceIterator.isFileReplayable("clickstream_log_123456"));
		assertFalse(SequentialLogFileSourceIterator.isFileReplayable("clickstream_log_1234"));
		assertFalse(SequentialLogFileSourceIterator.isFileReplayable("clickstream_log_current"));
		assertFalse(SequentialLogFileSourceIterator.isFileReplayable("clickstream_log12345"));
		assertFalse(SequentialLogFileSourceIterator.isFileReplayable("clickstreamlog12345"));
		assertFalse(SequentialLogFileSourceIterator.isFileReplayable("12345"));
		assertFalse(SequentialLogFileSourceIterator.isFileReplayable("clickstream_log_"));
		assertFalse(SequentialLogFileSourceIterator.isFileReplayable("CLICKSTREAM_LOG_12345"));
	}
	
	public void testInitializeWithExistingSources() throws Exception { 

		SequentialLogFileSourceIterator testIter = new SequentialLogFileSourceIterator(dir, new HAProxyLogParser(), true) {
			/**
			 * Avoid setting up any monitoring threads
			 */
			@Override
			protected void monitor(String srcDir, Set<String> exclusionSet) {
				assertEquals(srcDir, dir);
			}
		};
		
		File file3 = createTempFile(dir, "clickstream_log_00003");
		File file2 = createTempFile(dir, "clickstream_log_00002");
		File file1 = createTempFile(dir, "clickstream_log_00001");
		
		testIter.initializeWithExistingSources(dir);
		
		assertEquals(3, testIter.size());
		LogFileSource sources[] = testIter.toArray();
		
		// files should be sorted lexically
		assertEquals(file1.getCanonicalPath(), sources[0].getId());
		assertEquals(file2.getCanonicalPath(), sources[1].getId());
		assertEquals(file3.getCanonicalPath(), sources[2].getId());
	}
	
	public void testSequentialLogFileListenerFileCreatedHandlerLoadsFiles() throws IOException { 

		BlockingQueue<LogFileSource> sources = new LinkedBlockingQueue<LogFileSource>();
		SequentialLogFileListener listener = new SequentialLogFileListener(sources, new HAProxyLogParser());
		
		// create the initial 'previous' one matching (clickstream_log_12345 - 1)
		File initialFile = createTempFile(dir, "clickstream_log_12344");
		
		List<String> logFileCreationEvents = new ArrayList<String>();
		logFileCreationEvents.add("clickstream_log_12345");
		logFileCreationEvents.add("clickstream_log_12346");
		logFileCreationEvents.add("clickstream_log_12347");
		logFileCreationEvents.add("clickstream_log_12348");
		logFileCreationEvents.add("clickstream_log_12349");
		
		for (String name : logFileCreationEvents) { 
			// create the file so that the listener can verify that it exists
			File newFile = createTempFile(dir, name);
			// listener will try to load newFile.suffix - 1 (ie. for 12345 it will try to load 12344)
			listener.fileCreated(1, newFile.getParent(), newFile.getName());
		}

		/*
		 * Check that the specific items we expect to be loaded were loaded
		 */
		
		Set<String> expectedLoadedFiles = new HashSet<String>();
		expectedLoadedFiles.add(initialFile.getName());
		// we expect all but the last (largest sequence) one to be loaded
		expectedLoadedFiles.addAll(logFileCreationEvents);
		expectedLoadedFiles.remove(logFileCreationEvents.get(logFileCreationEvents.size() - 1));
		
		assertEquals("files 12344 - 12348 loaded ok", expectedLoadedFiles.size(), sources.size());
		
		Iterator<LogFileSource> it = sources.iterator();
		while (it.hasNext()) { 
			LogFileSource source = it.next();
			String name = new File(source.getId()).getName();
			assertTrue("failed to load filename " + name, expectedLoadedFiles.contains(name));
			expectedLoadedFiles.remove(name);
		}
	}

	private File createTempFile(String tmpDir, String fileName) throws IOException { 
		File tmpFile = new File(tmpDir, fileName);
		if (tmpFile.exists()) { 
			tmpFile.delete();
		}
		tmpFile.createNewFile();
		tmpFile.deleteOnExit();
		return tmpFile;
	}
	
	
	
	private class FileGeneratorThread extends Thread { 
		protected List<File> files = new ArrayList<File>();	
        public void deleteAllFiles() { 
        	for (File f : files) { 
        		if (f.exists()) { 
        			f.delete();
        		}
        	}
        }
	}
	
	private class NewFileGenerator extends FileGeneratorThread {	
        public void run() {
        	try {
        		Thread.sleep(1000);
       			File f = new File(new File(dir), prefix + suffix2); // yes, we know dir is the default for a tempfile, just being explicit.
       			f.createNewFile();
       			f.deleteOnExit();
       			files.add(f);
	        } catch(Exception e) {
        		fail("file creation failed");
        	}
        }
	}
	
	private class TestFileGenerator extends FileGeneratorThread {
        public void run()  {
        	try {
 	        	File f = new File(new File(dir), prefix + suffix1);  // yes, we know dir is the default for a tempfile, just being explicit.
 	        	f.createNewFile();
 	        	f.deleteOnExit();
 	        	files.add(f);
        	} catch(Exception e) {
        		fail("file creation failed");
        	}
        }
	}
}