package org.eclipse.jetty.nosql.kvs;

//========================================================================
//Copyright (c) 2011 Intalio, Inc.
//Copyright (c) 2012 Geisha Tokyo Entertainment, Inc.
//------------------------------------------------------------------------
//All rights reserved. This program and the accompanying materials
//are made available under the terms of the Eclipse Public License v1.0
//and Apache License v2.0 which accompanies this distribution.
//The Eclipse Public License is available at
//http://www.eclipse.org/legal/epl-v10.html
//The Apache License v2.0 is available at
//http://www.opensource.org/licenses/apache2.0.php
//You may elect to redistribute this code under either of these licenses.
//========================================================================

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.SessionManager;
import org.eclipse.jetty.server.handler.ContextHandler;
import org.eclipse.jetty.server.session.AbstractSessionIdManager;
import org.eclipse.jetty.server.session.SessionHandler;
import org.eclipse.jetty.util.log.Log;
import org.eclipse.jetty.util.log.Logger;

/**
 * Based partially on the jdbc session id manager...
 * 
 * Theory is that we really only need the session id manager for the local
 * instance so we have something to scavenge on, namely the list of known ids
 * 
 * this class has a timer that runs at the scavenge delay that runs a query for
 * all id's known to this node and that have and old accessed value greater then
 * the scavengeDelay.
 * 
 * these found sessions are then run through the invalidateAll(id) method that
 * is a bit hinky but is supposed to notify all handlers this id is now DOA and
 * ought to be cleaned up. this ought to result in a save operation on the
 * session that will change the valid field to false (this conjecture is
 * unvalidated atm)
 */
public abstract class KeyValueStoreSessionIdManager extends AbstractSessionIdManager {
	private final static Logger log = Log.getLogger("org.eclipse.jetty.nosql.kvs.KeyValueStoreSessionIdManager");

	protected Server _server;

	protected long _defaultExpiry = TimeUnit.MINUTES.toMillis(30); // 30 minutes

	protected String _keyPrefix = "";
	protected String _keySuffix = "";
	protected KeyValueStoreClientPool _pool = null;
	protected IKeyValueStoreClient[] _clients;
	protected String _serverString = "";
	protected int _timeoutInMs = 1000;
	protected int _poolSize = 2;

	private Cache<String, HttpSession> _cache;

	public KeyValueStoreSessionIdManager(Server server, String serverString) throws IOException {
		super(new Random());
		this._server = server;
		this._serverString = serverString;
	}

	protected abstract AbstractKeyValueStoreClient newClient(String serverString);

	public void setScavengePeriod(long scavengePeriod) {
		this._defaultExpiry = scavengePeriod;
	}

	@Override
	protected void doStart() throws Exception {
		log.info("starting...");
		super.doStart();

		_clients = new IKeyValueStoreClient[_poolSize];
		for (int i = 0; i < _poolSize; i++) {
			_clients[i] = createClient();
		}
		_pool = new KeyValueStoreClientPool(_clients);

		if (this._defaultExpiry > 0) {
			this._cache = CacheBuilder.newBuilder()
					.expireAfterAccess(this._defaultExpiry, TimeUnit.MILLISECONDS)
					.removalListener(new RemovalListener<Object, HttpSession>() {
						public void onRemoval(final RemovalNotification<Object, HttpSession> objectObjectRemovalNotification) {
							HttpSession session = objectObjectRemovalNotification.getValue();
							if (session != null) {
								log.debug("Remove from cache " + session.getId());
								try {
									if (System.currentTimeMillis() - session.getLastAccessedTime() > _defaultExpiry) {
										log.info("Session timeout, invalidating session " + session.getId());
										session.invalidate();
									}
								} catch (Exception e) {
									log.warn("Failed to invalidate session " + session.getId(), e);
								}
							}
						}
					})
					.build();
		} else {
			this._cache = CacheBuilder.newBuilder()
					.build();
		}

		log.info("started.");
	}

	@Override
	protected void doStop() throws Exception {
		log.info("stopping...");

		for (int i = 0; i < _poolSize; i++) {
			destroyClient(_clients[i]);
		}
		_clients = null;
		_pool = null;
		_cache = null;
		super.doStop();

		log.info("stopped.");
	}

	private IKeyValueStoreClient createClient() throws Exception {
		IKeyValueStoreClient client = newClient(_serverString);
		if (client == null) {
			throw new IllegalStateException("newClient(" + _serverString + ") returns null.");
		}
		client.establish();
		return client;
	}

	private void destroyClient(IKeyValueStoreClient client) throws Exception {
		if (client != null) {
			client.shutdown();
		}
	}

	/* ------------------------------------------------------------ */
	/**
	 * is the session id known to memcached, and is it valid
	 */
	public boolean idInUse(String idInCluster) {
		byte[] dummy = idInCluster.getBytes(); // dummy string for reserving key
		boolean exists = ! addKey(idInCluster, dummy);
		// do not check the validity of the session since
		// we do not save invalidated sessions anymore.

		_cache.getIfPresent(idInCluster);
		return exists;
	}

	/* ------------------------------------------------------------ */
	public void addSession(HttpSession session) {
		if (session == null) {
			return;
		}
		_cache.put(session.getId(), session);
		log.debug("addSession:" + session.getId());
	}

	/* ------------------------------------------------------------ */
	public void removeSession(HttpSession session) {
		if (session == null) {
			return;
		}
		_cache.invalidate(session.getId());
		log.debug("removeSession:" + session.getId());
	}

	/* ------------------------------------------------------------ */
	public void invalidateAll(String sessionId) {
		// tell all contexts that may have a session object with this id to
		// get rid of them
		Handler[] contexts = _server.getChildHandlersByClass(ContextHandler.class);
		for (int i = 0; contexts != null && i < contexts.length; i++) {
			SessionHandler sessionHandler = ((ContextHandler) contexts[i]).getChildHandlerByClass(SessionHandler.class);
			if (sessionHandler != null) {
				SessionManager manager = sessionHandler.getSessionManager();
				if (manager != null && manager instanceof KeyValueStoreSessionManager) {
					((KeyValueStoreSessionManager) manager).invalidateSession(sessionId);
				}
			}
		}
		_cache.invalidate(sessionId);
	}

	/* ------------------------------------------------------------ */
	public String getClusterId(String nodeId) {
		if (nodeId == null) {
			return null;
		}
		int dot = nodeId.lastIndexOf('.');
		return (dot > 0) ? nodeId.substring(0, dot) : nodeId;
	}

	/* ------------------------------------------------------------ */
	public String getNodeId(String clusterId, HttpServletRequest request) {
		if (clusterId == null) {
			return null;
		}
		if (_workerName != null) {
			return clusterId + '.' + _workerName;
		}
		return clusterId;
	}

	protected String mangleKey(String key) {
		return _keyPrefix + key + _keySuffix;
	}

	protected byte[] getKey(String idInCluster) {
		log.debug("getKey: id=" + idInCluster);
		byte[] raw = null;
		try {
			raw = _pool.get().get(mangleKey(idInCluster));
		} catch (KeyValueStoreClientException error) {
			log.warn("unable to get key: id=" + idInCluster, error);
		}

		_cache.getIfPresent(idInCluster);
		return raw;
	}

	protected boolean setKey(String idInCluster, byte[] raw) {
		return setKey(idInCluster, raw, getDefaultExpiry());
	}

	protected boolean setKey(String idInCluster, byte[] raw, int expiry) {
		if (expiry < 0) {
			expiry = 0; // 0 means forever
		}
		log.debug("setKey: id=" + idInCluster + ", expiry=" + expiry);
		boolean result = false;
		try {
			result = _pool.get().set(mangleKey(idInCluster), raw, expiry);
		} catch (KeyValueStoreClientException error) {
			log.warn("unable to set key: id=" + idInCluster, error);
		}

		_cache.getIfPresent(idInCluster);
		return result;
	}

	protected boolean addKey(String idInCluster, byte[] raw) {
		return addKey(idInCluster, raw, getDefaultExpiry());
	}

	protected boolean addKey(String idInCluster, byte[] raw, int expiry) {
		if (expiry < 0) {
			expiry = 0; // 0 means forever
		}
		log.debug("add: id=" + idInCluster + ", expiry=" + expiry);
		boolean result = false;
		try {
			result = _pool.get().add(mangleKey(idInCluster), raw, expiry);
		} catch (KeyValueStoreClientException error) {
			log.warn("unable to add key: id=" + idInCluster, error);
		}

		_cache.getIfPresent(idInCluster);
		return result;
	}

	protected boolean deleteKey(String idInCluster) {
		log.debug("delete: id=" + idInCluster);
		boolean result = false;
		try {
			result = _pool.get().delete(mangleKey(idInCluster));
			if (result == false) {
				byte[] raw = _pool.get().get(mangleKey(idInCluster));
				if (raw == null) {
					result = true; // expired already
				}
			}
		} catch (KeyValueStoreClientException error) {
			log.warn("unable to delete key: id=" + idInCluster, error);
		}

		_cache.invalidate(idInCluster);
		return result;
	}

	public int getDefaultExpiry() {
		return (int) TimeUnit.MILLISECONDS.toSeconds(_defaultExpiry);
	}

	public void setDefaultExpiry(int defaultExpiry) {
		this._defaultExpiry = TimeUnit.SECONDS.toMillis(defaultExpiry);
	}

	public String getKeyPrefix() {
		return _keyPrefix;
	}

	public void setKeyPrefix(String keyPrefix) {
		this._keyPrefix = keyPrefix;
	}

	public String getKeySuffix() {
		return _keySuffix;
	}

	public void setKeySuffix(String keySuffix) {
		this._keySuffix = keySuffix;
	}

	public String getServerString() {
		return _serverString;
	}

	public void setServerString(String serverString) {
		this._serverString = serverString;
	}

	public int getTimeoutInMs() {
		return _timeoutInMs;
	}

	public void setTimeoutInMs(int timeoutInMs) {
		this._timeoutInMs = timeoutInMs;
	}

	public int getPoolSize() {
		return _poolSize;
	}

	public void setPoolSize(int poolSize) {
		this._poolSize = poolSize;
	}

	/**
	 * @deprecated from 0.3.0. this is false by default and is not an option.
	 */
	@Deprecated
	public void setSticky(boolean sticky) { // TODO: remove
		log.warn("deprecated setter `setSticky' was called. this will be removed in future release.");
	}

	/**
	 * @deprecated from 0.3.0. this is false by default and is not an option.
	 */
	@Deprecated
	public boolean isSticky() { // TODO: remove
		return false;
	}
}
