package logreplay.consumer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import logreplay.producer.Producer;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpVersion;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.conn.scheme.PlainSocketFactory;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.scheme.SchemeRegistry;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.SingleClientConnManager;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.params.HttpParams;
import org.apache.http.params.HttpProtocolParams;
import org.apache.log4j.Logger;


public class HttpGetConsumer extends Consumer<HttpGet>  {
	private final Logger logger = Logger.getLogger(HttpGetConsumer.class);
	
	private final HttpClient http11Client;

	public HttpGetConsumer(Producer<HttpGet> producer) {
		super(producer);
		this.http11Client = createThreadSafeClient(HttpVersion.HTTP_1_1);
	}
	
	protected static HttpClient createThreadSafeClient(HttpVersion version) {
	    HttpParams params = new BasicHttpParams();
	    params.setParameter("http.socket.timeout", new Integer(1000 * 60 * 5)); // 5 min timeout on socket connection (SO_TIMEOUT)
		params.setParameter("http.connection.timeout", new Integer(0)); // no timeout on http connection
		params.setParameter("http.connection-manager.timeout", new Long(0)); // no timeout on http connection manager
	    
        HttpProtocolParams.setVersion(params, version);      
        
        SchemeRegistry schemeRegistry = new SchemeRegistry();
        schemeRegistry.register(new Scheme("http", PlainSocketFactory.getSocketFactory(), 80));
		
		return new DefaultHttpClient(new SingleClientConnManager(params, schemeRegistry), params);
    }

	@Override
	protected long execute(HttpGet request) throws ConsumerException { 

		if (logger.isTraceEnabled()) { 
			logger.trace("request: " + request.toString());
		}

		HttpResponse response;
		HttpEntity entity;
		int statusCode;
		long size;
		long requestStartTime, requestEndTime, parseTime;

		requestStartTime = System.currentTimeMillis();

		try { 
			response = http11Client.execute(request);
			requestEndTime = System.currentTimeMillis();

			statusCode = response.getStatusLine().getStatusCode();
			if (statusCode == -1) {
				//handle failure, log, re-request, and determine if time to throttle sending	        	
				logger.warn("failed to issue request (statusCode: " + statusCode + ")");
			}

			entity = response.getEntity();
			if (entity != null) {
				//Required to read the stream
				size = getContentSize(entity);
				parseTime = System.currentTimeMillis();
				if (logger.isTraceEnabled()) {
					logger.trace("request: '" + (requestEndTime - requestStartTime) + "', data: '" + (parseTime - requestEndTime) + "', statusCode: "  + statusCode + "', size: " + size);
				}

			} else { 
				parseTime = requestEndTime;
			}

		} catch (ClientProtocolException cpe) { 
			throw new ConsumerException("failed to execute http client", cpe);
		} catch (IOException ioe) { 
			throw new ConsumerException("failed to get content size from response", ioe);
		}

		if (logger.isTraceEnabled()) {
			logger.trace("issued request");
		}

		return parseTime - requestStartTime;
	}
	
	@Override
	public void shutdown() { 
	    http11Client.getConnectionManager().shutdown();
	}

	long getContentSize(HttpEntity entity) throws IOException {
		InputStream stream = null;
		BufferedReader reader = null;
		try { 
			long size = 0;
			stream = entity.getContent();
			reader = new BufferedReader(new InputStreamReader(stream));
			for (long bufferSize = 4096, skip = bufferSize; skip == bufferSize; size += (skip = reader.skip(bufferSize)))
				; // noop
			return size;
		} finally { 
			reader.close();
			stream.close();
		}
    }
	
}
