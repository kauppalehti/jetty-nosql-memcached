package org.eclipse.jetty.nosql.kvs;

public class KeyValueStoreClientPool {
	private volatile Node<IKeyValueStoreClient> current;

	public KeyValueStoreClientPool(final IKeyValueStoreClient... clients) {
		if (clients == null || clients.length < 1) {
			throw new IllegalArgumentException("Clients provided for KeyValueStoreClientPool cannot be null or zero-length");
		}

		Node<IKeyValueStoreClient> node = new Node<IKeyValueStoreClient>(clients[clients.length - 1], null);
		Node<IKeyValueStoreClient> last = node;
		for (int i = (clients.length - 2); i >= 0; i--) {
			node = new Node<IKeyValueStoreClient>(clients[i], node);
		}
		last.next = node;
		current = node;
	}

	public IKeyValueStoreClient get() {
		IKeyValueStoreClient client = current.client;
		current = current.next;
		return client;
	}

	static private class Node<IKeyValueStoreClient> {
		final IKeyValueStoreClient client;
		Node<IKeyValueStoreClient> next;

		Node(final IKeyValueStoreClient client, final Node<IKeyValueStoreClient> next) {
			this.client = client;
			this.next =next;
		}
	}
}

