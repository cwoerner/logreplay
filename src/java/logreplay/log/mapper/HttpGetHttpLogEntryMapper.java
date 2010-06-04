package logreplay.log.mapper;

import java.net.URI;

import logreplay.log.HttpLogEntry;

import org.apache.http.client.methods.HttpGet;
import org.apache.log4j.Logger;


/**
 * Encapsulates the logic for mapping the log entries to an executable HttpGet command.
 * 
 * Makes the Producer agnostic of the endpoint he's producing log entries for.
 *  
 * @author cwoerner
 *
 */
public class HttpGetHttpLogEntryMapper implements HttpLogEntryMapper<HttpGet> {

	private final String destination;
	
	public HttpGetHttpLogEntryMapper(String destination) { 
		this.destination = destination;
	}
	
	protected URI createURIForPath(String path) {
		if (null == path || "".equals(path.trim())) { 
			throw new IllegalArgumentException("invalid path: '" + path + "'");
		}
		
		if (path.charAt(0) != '/') { 
			path = "/" + path;
		}
		return URI.create(this.destination + path);
	}
	
	/**
	 * Logic for mapping an entry to to an HttpGet command.
	 * Note that the act of creating and issuing an HttpGet
	 * command does induce a DNS lookup.  Can set DNS cache
	 * properties if this becomes a performance bottleneck.
	 * 
	 * @param httpReq
	 * @return
	 */
	public HttpGet map(HttpLogEntry httpReq) {
		HttpGet get = null;
		try { 
			get = new HttpGet(createURIForPath(httpReq.getPath()));
			for (HttpLogEntry.Header hdr : httpReq.getHeaders()) { 
				get.addHeader(hdr.name, hdr.value);
			}
		} catch (Exception e) { 
			Logger.getLogger(getClass()).error("failed to create HttpGet for log entry '" + httpReq.getPath() + "'", e);
		}
		
		return get;
	}
}
