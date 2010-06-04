package logreplay.log.parser;

import java.io.Reader;
import java.text.ParseException;

import logreplay.log.HttpLogEntry;
import logreplay.log.StreamingHttpLogEntryIterator;


/**
 * Iterate over log data and parse each line into HttpLogEntry objects.
 * @author cwoerner
 *
 */
public interface LogFileSourceParser {
	
	public StreamingHttpLogEntryIterator parse(Reader reader) throws ParseException;
	public HttpLogEntry parseLine(String line) throws ParseException;
}