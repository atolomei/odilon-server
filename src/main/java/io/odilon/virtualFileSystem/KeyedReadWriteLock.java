/*
 * Odilon Object Storage
 * (c) kbee 
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.odilon.virtualFileSystem;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * <p>
 * Handle to a {@link ReentrantReadWriteLock} registered by key in a map whose
 * idle entries are evicted asynchronously by the {@link OdilonLockService}
 * cleaner.
 * </p>
 * <p>
 * Without this handle there is a race: a thread obtains lock {@code L1} from
 * the map, the cleaner {@code tryLock()}s {@code L1} (still idle), the thread
 * blocks on {@code L1}, the cleaner removes {@code L1} from the map and
 * unlocks it, and the thread ends up holding an orphan lock. A subsequent
 * {@code unlock()} that resolves the lock by key gets a different instance and
 * throws {@link IllegalMonitorStateException} (and mutual exclusion is lost
 * meanwhile).
 * </p>
 * <p>
 * Every acquisition here validates, after acquiring, that the lock is still
 * the one registered for the key; if not it releases it and retries. A held
 * and registered lock can not be evicted (the cleaner's {@code tryLock()}
 * fails), so {@code unlock()} always resolves to the instance that is held.
 * </p>
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
public final class KeyedReadWriteLock implements ReadWriteLock {

	private final ConcurrentMap<String, ReentrantReadWriteLock> map;
	private final String key;
	private final Lock readLock;
	private final Lock writeLock;

	public KeyedReadWriteLock(ConcurrentMap<String, ReentrantReadWriteLock> map, String key) {
		this.map = map;
		this.key = key;
		this.readLock = new KeyedLock(true);
		this.writeLock = new KeyedLock(false);
	}

	@Override
	public Lock readLock() {
		return this.readLock;
	}

	@Override
	public Lock writeLock() {
		return this.writeLock;
	}

	public String getKey() {
		return this.key;
	}

	private ReentrantReadWriteLock resolve() {
		return this.map.computeIfAbsent(this.key, k -> new ReentrantReadWriteLock());
	}

	private boolean isRegistered(ReentrantReadWriteLock lock) {
		return this.map.get(this.key) == lock;
	}

	private final class KeyedLock implements Lock {

		private final boolean read;

		KeyedLock(boolean read) {
			this.read = read;
		}

		private Lock view(ReentrantReadWriteLock rw) {
			return this.read ? rw.readLock() : rw.writeLock();
		}

		@Override
		public void lock() {
			for (;;) {
				ReentrantReadWriteLock rw = resolve();
				Lock l = view(rw);
				l.lock();
				if (isRegistered(rw))
					return;
				/** evicted by the cleaner while waiting -> retry with the current one */
				l.unlock();
			}
		}

		@Override
		public void lockInterruptibly() throws InterruptedException {
			for (;;) {
				ReentrantReadWriteLock rw = resolve();
				Lock l = view(rw);
				l.lockInterruptibly();
				if (isRegistered(rw))
					return;
				l.unlock();
			}
		}

		@Override
		public boolean tryLock() {
			for (;;) {
				ReentrantReadWriteLock rw = resolve();
				Lock l = view(rw);
				if (!l.tryLock())
					return false;
				if (isRegistered(rw))
					return true;
				l.unlock();
			}
		}

		@Override
		public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
			final long deadline = System.nanoTime() + unit.toNanos(time);
			for (;;) {
				ReentrantReadWriteLock rw = resolve();
				Lock l = view(rw);
				long remaining = Math.max(0L, deadline - System.nanoTime());
				if (!l.tryLock(remaining, TimeUnit.NANOSECONDS))
					return false;
				if (isRegistered(rw))
					return true;
				l.unlock();
			}
		}

		@Override
		public void unlock() {
			ReentrantReadWriteLock rw = map.get(key);
			if (rw == null)
				throw new IllegalMonitorStateException("lock not held -> " + key);
			view(rw).unlock();
		}

		@Override
		public Condition newCondition() {
			throw new UnsupportedOperationException("newCondition is not supported");
		}
	}
}
