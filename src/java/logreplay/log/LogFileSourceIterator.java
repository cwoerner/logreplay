package logreplay.log;

import java.util.Iterator;
import java.util.Set;

import logreplay.log.source.LogFileSource;


/**
 * 
 * @author cwoerner
 *
 */
public abstract class LogFileSourceIterator implements Iterator<LogFileSource> {

	/**
	 * Opportunity for initialization code.  Helps keep constructors side-effect free.
	 */
	protected void initialize() {
		
	}
	
	public abstract Set<String> getInitialFiles();
	
	/**
	 * Return but don't remove the head of the internal queue.
	 * @return
	 */
	public abstract LogFileSource peek();
	
	/**
	 * Return a snapshot of the items in the internal iterator queue.
	 * @return
	 */
	public abstract LogFileSource[] toArray();
	
	/**
	 * Do cleanup like joining monitor threads, etc.
	 */
	public abstract void shutdown();
}
