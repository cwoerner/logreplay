package logreplay.log.parser.haproxy;

import java.io.BufferedReader;
import java.io.Reader;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import logreplay.log.HttpLogEntry;
import logreplay.log.StreamingHttpLogEntryIterator;
import logreplay.log.parser.LogFileSourceParser;

import org.apache.log4j.Logger;


/**
 * Given an HAProxy log, this class parses the input returning an StreamingHttpLogEntryIterator.
 * 
 * Example log entry follows.  Note that our logs include original ip address, referer header and user agent headers. 
 * 	
 * 		Apr 21 21:47:10 localhost.localdomain haproxy[9898]: 127.0.0.1:56814 [21/Apr/2010:21:47:09.509] leads-http-fe leads-http-be/www1 22/0/98/102/503 200 390 - - ---- 35/35/32/32/0 0/0 {http://www.foo.com/path/to/something/|Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_5_8; en-us) AppleWebKit/531.22.7 (KHTML, like Gecko) Version/4.0.5 Safari/531.22.7|} "GET /somepath/to/someresource HTTP/1.1"
 * 
 * Example haproxy httplog configuration for the frontend section would be:
 * <pre>
 *   frontend leads-http-fe
 *       bind            *:80
 *       mode            http
 *       log             global
 *       option          httplog
 *       option          dontlognull
 *       option          httpclose
 *       option          forwardfor
 *       clitimeout      30000
 *       default_backend leads-http-be
 *
 *       capture request  header Referer len 192
 *       capture request  header User-Agent len 128
 *       capture request  header X-Forwarded-For len 192
 * </pre>
 * @author cwoerner
 *
 */
public class HAProxyLogParser implements LogFileSourceParser {
	
	private final Logger logger = Logger.getLogger(HAProxyLogParser.class);
	
	private final Pattern logFormatMatcher = Pattern.compile(".*?haproxy\\[\\d+\\]:\\s+((?:\\d{1,3})\\.(?:\\d{1,3})\\.(?:\\d{1,3}).(?:\\d{1,3})):\\d+\\s+\\[(.*?)\\].*?\\{(.*?)\\|(.*)\\|(.*?)\\}\\s+\"(?:GET|POST|HEAD)\\s+(.*?)\\s+HTTP.*$");
	private final DateFormat dateFormat  = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss.SSS");

	/**
	 * Assumes that line has already been stripped of trailing \n.
	 */
	public HttpLogEntry parseLine(String line) throws ParseException { 
		if (null == line || "".equals(line)) {
			return null;
		}

		HttpLogEntry httpLogEntry = null;
		
		Matcher matcher = logFormatMatcher.matcher(line);
		if (!matcher.matches()) { 
			logger.warn("input line failed to match pattern [ " + line + " ]");
		} else { 
			String ip = matcher.group(1);
			String time = matcher.group(2);
			String ref = matcher.group(3);
			String ua = matcher.group(4);
			String xff = matcher.group(5);
			String uri = matcher.group(6);
			
			if (null == xff || "".equals(xff)) { 
				xff = ip;
			} else { 
				xff = xff + "," + ip;
			}
			
			Date date = dateFormat.parse(time);
			
			httpLogEntry = new HttpLogEntry(date, uri);
			httpLogEntry.addHeader("Referer", ref);
			httpLogEntry.addHeader("User-Agent", ua);
			httpLogEntry.addHeader("X-Forwarded-For", xff);
		}
		
		return httpLogEntry;
	}
	
	public StreamingHttpLogEntryIterator parse(Reader reader) throws ParseException { 
		BufferedReader buffRead = (reader instanceof BufferedReader) ? (BufferedReader)reader : new BufferedReader(reader);		
		return new StreamingHttpLogEntryIterator(buffRead, this);
	}

}
