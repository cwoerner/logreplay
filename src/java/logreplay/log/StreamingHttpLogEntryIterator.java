/**
 * 
 */
package logreplay.log;

import java.io.BufferedReader;
import java.io.IOException;
import java.text.ParseException;
import java.util.Iterator;
import java.util.NoSuchElementException;

import logreplay.log.parser.LogFileSourceParser;

import org.apache.log4j.Logger;


/**
 * Iterate over log data from a Reader.
 * @author cwoerner
 *
 */
public class StreamingHttpLogEntryIterator implements Iterator<HttpLogEntry> {

	private static final Logger logger = Logger.getLogger(StreamingHttpLogEntryIterator.class);
	
	private final BufferedReader reader;
	private final LogFileSourceParser entParser;
	
	public StreamingHttpLogEntryIterator(BufferedReader reader, LogFileSourceParser entParser) { 
		this.reader = reader;
		this.entParser = entParser;
	}
	
	private String curLine = null;

	/**
	 * Indicates whether or not there are more items.  Blocking until current log line can be parsed completely.  Synchronized with next().
	 */
	public synchronized boolean hasNext() {
		if (null != curLine) {
			// support consecutive calls to hasNext() without losing data
			return true;
		}
		
		boolean hasNext = false;
		
		try { 
			hasNext = advance() != null;
		} catch (IOException ioe) { 
			throw new RuntimeException("failed to check if reader is ready", ioe);
		} finally { 
			if (!hasNext) { 
				try { 
					this.reader.close();
				} catch (IOException ioe) { 
					logger.error("io exception closing reader", ioe);
				}
			}
		}
		
		return hasNext;
	}
	
	private String advance() throws IOException { 
		return (curLine = this.reader.readLine());
	}

	/**
	 * Take the next item and move the internal cursor to the next log entry.  Non-blocking.  Synchronized with hasNext().
	 */
	public synchronized HttpLogEntry next() {
		if (null == curLine) { 
			throw new NoSuchElementException();
		}
		
		try { 
			return entParser.parseLine(curLine);			
		} catch (ParseException pe) {
			logger.error("failed to parse current line", pe);
			return null;
		} finally { 
			this.curLine = null;
		}
	}

	/**
	 * Unsupported.
	 */
	public void remove() {
		throw new UnsupportedOperationException("remove not supported");
	} 
}