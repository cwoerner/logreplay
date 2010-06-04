package logreplay.log.source;

import java.io.IOException;
import java.io.Reader;
import java.text.ParseException;
import java.util.Iterator;

import logreplay.log.HttpLogEntry;
import logreplay.log.parser.LogFileSourceParser;


/**
 * Abstract representation of a log file.  Subclasses can provide StringReaders, 
 * FileReaders, etc. in the getReader() implementation.
 * 
 * @author cwoerner
 *
 */
public abstract class LogFileSource {
	private final String id;
	private final LogFileSourceParser parser;
	
	public LogFileSource(String id, LogFileSourceParser parser) { 
		this.id = id;
		this.parser = parser; 
	}
	
	/**
	 * Implementation specific identifier.
	 * @return
	 */
	public String getId() { 
		return this.id;
	}
	
	public boolean exists() { 
		return true;
	}
	
	/**
	 * Returns an iterator containing entries parsed from the log file.  The iterator
	 * may iterate over the underlying file in a streaming fashion.
	 * 
	 * @return
	 * @throws IOException
	 */
	public Iterator<HttpLogEntry> getIterator() throws IOException, ParseException { 
		return this.parser.parse(getReader());
	}

	
	/**
	 * Provide a Reader which provides log data.
	 * @return
	 * @throws IOException
	 */
	public abstract Reader getReader() throws IOException;

}
