package logreplay.log.mapper;

import logreplay.log.HttpLogEntry;

/**
 * Map an HttpLogEntry to some other type.
 * @author cwoerner
 *
 * @param <T>
 */
public interface HttpLogEntryMapper<T> {

	/**
	 * Logic for mapping an entry to to an HttpGet command.
	 * @param httpReq
	 * @return
	 */
	public T map(HttpLogEntry httpReq);

}