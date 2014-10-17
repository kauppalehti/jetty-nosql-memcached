package org.eclipse.jetty.nosql.memcached;

import org.eclipse.jetty.server.session.AbstractSessionInvalidateAndCreateTest;
import org.eclipse.jetty.server.session.AbstractTestServer;
import org.junit.Test;

public class AbstractMemcachedSessionInvalidateAndCreateTest extends AbstractSessionInvalidateAndCreateTest {

	public AbstractTestServer createServer(int port, int max, int scavenge) {
		return new MemcachedTestServer(port, max, scavenge);
	}

	@Test
	public void testSessionScavenge() throws Exception {
		super.testSessionScavenge();
	}
}
