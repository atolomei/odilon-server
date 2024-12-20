/*
 * Odilon Object Storage
 * (C) Novamens 
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
package io.odilon.service;

import java.util.concurrent.atomic.AtomicBoolean;

import io.odilon.log.Logger;
import io.odilon.util.Check;

/**
 * <p>
 * Simple abstract class that can execute a cleanup process regularly
 * </p>
 * <p>
 * Subclasses must implement the method {@code cleanUp}
 * </p>
 * 
 * @see {@link OdilonLockService}
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 * 
 */
public abstract class PoolCleaner implements Runnable {

    static public Logger logger = Logger.getLogger(PoolCleaner.class.getName());

    static final long DEFAULT_SLEEP_TIME = 1 * 60 * 1000; // 1 minute

    private AtomicBoolean exit = new AtomicBoolean(false);
    private Thread thread;

    public PoolCleaner() {
    }

    public abstract void cleanUp();

    public boolean exit() {
        return this.exit.get();
    }

    public void sendExitSignal() {
        this.exit.set(true);
        if (this.thread != null)
            this.thread.interrupt();
    }

    public long getSleepTimeMillis() {
        return DEFAULT_SLEEP_TIME;
    }

    @Override
    public void run() {
        Check.checkTrue(getSleepTimeMillis() > 100, "sleep time must be > 100 milisecs -> " + String.valueOf(getSleepTimeMillis()));
        this.thread = Thread.currentThread();
        synchronized (this) {
            while (!exit()) {
                try {
                    Thread.sleep(getSleepTimeMillis());
                    cleanUp();
                } catch (InterruptedException e) {
                }
            }
        }
    }
}
