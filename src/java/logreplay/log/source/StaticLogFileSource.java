package logreplay.log.source;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;

import logreplay.log.parser.LogFileSourceParser;


/**
 * LogFileSource implementation for an actual file on disk.
 * @author cwoerner
 *
 */
public class StaticLogFileSource extends LogFileSource {

	/**
	 * Accepts a fileName resolving to a valid log file.  Provides a default LoggingLogFileSourceTracker for the tracker.
	 * @param fileName
	 * @param parser
	 * @throws IOException
	 */
	public StaticLogFileSource(String fileName, LogFileSourceParser parser) throws IOException { 
		this(new File(fileName), parser);
	}
	
	/**
	 * Provides a default LoggingLogFileSourceTracker for the tracker.
	 * @param file
	 * @param parser
	 * @throws IOException
	 */
	public StaticLogFileSource(File file, LogFileSourceParser parser) throws IOException { 
		super(file.getCanonicalPath(), parser);
	}
	
		
	@Override
	public Reader getReader() throws FileNotFoundException { 
		return new BufferedReader(new InputStreamReader(new FileInputStream(getId())));
	}
	
	public File getFile() { 
		return new File(getId());
	}

	@Override
	public boolean exists() { 
		return getFile().exists();
	}
}
