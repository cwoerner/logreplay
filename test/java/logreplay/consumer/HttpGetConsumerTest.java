package logreplay.consumer;

import junit.framework.TestCase;
import logreplay.consumer.HttpGetConsumer;

import org.apache.http.HttpVersion;
import org.apache.http.client.HttpClient;
import org.apache.http.conn.params.ConnManagerParams;

public class HttpGetConsumerTest extends TestCase {

	protected void setUp() throws Exception {
		super.setUp();
	}

	protected void tearDown() throws Exception {
		super.tearDown();
	}

	public void testCreateThreadSafeClient() {
		HttpClient client = HttpGetConsumer.createThreadSafeClient(HttpVersion.HTTP_1_1);
		assertNotNull(client);
		
		assertEquals(new Integer(1000 * 60 * 5), (Integer) client.getParams().getParameter("http.socket.timeout"));
		assertEquals(new Integer(0), (Integer) client.getParams().getParameter("http.connection.timeout"));
		assertEquals(new Long(0), (Long) client.getParams().getParameter("http.connection-manager.timeout"));
		
		assertEquals(0, ConnManagerParams.getTimeout(client.getParams()));
	}

}
