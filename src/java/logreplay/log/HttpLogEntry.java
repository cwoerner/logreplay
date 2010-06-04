/**
 * 
 */
package logreplay.log;

import java.text.ParseException;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * Represents the http request parsed from a web log.
 * @author cwoerner
 *
 */
public class HttpLogEntry { 
	private final String path;
	private final Map<String, HttpLogEntry.Header> headers = new HashMap<String, HttpLogEntry.Header>();
	private final Date date;
	
	public HttpLogEntry(Date date, String path) { 
		this.path = path;
		this.date = date;
	}
	
	public class Header {
		public final String name;
		public final String value;
		public Header(String name, String value){ 
			this.name = name;
			this.value = value;
		}
	}
	
	public String getPath() { 
		return path;
	}
	
	public long getTimeMillis() throws ParseException {
	    return date.getTime();
	}
	
	public void addHeader(HttpLogEntry.Header hdr) { 
		headers.put(hdr.name, hdr);
	}
	public void addHeader(String name, String value) { 
		addHeader(new Header(name, value));
	}
	
	public HttpLogEntry.Header getHeader(String name) { 
		return headers.get(name);
	}
	
	public Collection<HttpLogEntry.Header> getHeaders() {
		return headers.values();
	}
}