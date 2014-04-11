package org.eclipse.jetty.nosql.memcached;

import org.eclipse.jetty.server.session.AbstractSessionExpiryTest;
import org.eclipse.jetty.server.session.AbstractTestServer;
import org.junit.Test;

public abstract class AbstractMemcachedSessionExpiryTest extends AbstractSessionExpiryTest
{
	public AbstractTestServer createServer(int port) {
		return new MemcachedTestServer(port);
	}

	@Test
	public void testSessionNotExpired() throws Exception {
		super.testSessionNotExpired();
	}

	@Test
	public void testSessionExpiry() throws Exception {
		super.testSessionExpiry();
	}
}
