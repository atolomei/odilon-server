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
package io.odilon.virtualFileSystem.raid6;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import jakarta.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.annotation.JsonIgnore;

import io.odilon.client.util.NumberFormatter;
import io.odilon.log.Logger;
import io.odilon.model.RedundancyLevel;
import io.odilon.model.ServerConstant;
import io.odilon.model.ServiceStatus;
import io.odilon.service.BaseService;
import io.odilon.service.ServerSettings;

/**
 * <p>
 * The pool buffers are used only by RAID 6 ({@link RAIDSixEncoder} and {@link RAIDSixDecoder}).
 * </p>
 <p>
 * <p>The pool requires a minimum of one 32 MB buffer for each disk used by the RAID 6 configuration (ie. 3, 6, 12, 24, 48). 
 * Default value is 20 buffers or the number of disks, whatever value is larger.
 * The total buffers can be set in odilon.properties (parameter -> <i>raid6.buffers</i>)
 * </p>
 * <p>When Odilon is used in RAID 0 or RAID 1 this service is not used, 
 * and no memory is allocated for buffers, regardless of the parameter set in odilon.properties.
 * </p> 
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
@Service
public class BufferPoolService extends BaseService {

	@SuppressWarnings("unused")
	static private Logger logger = Logger.getLogger(BufferPoolService.class.getName());
	static private Logger startuplogger = Logger.getLogger("StartupLogger");

	@JsonIgnore
	private final BlockingQueue<byte[]> pool;

	@JsonIgnore
	@Autowired
	private final ServerSettings serverSettings;

	private int poolSize;
	private int bufferSize;

	@JsonIgnore
	private AtomicBoolean initialized = new AtomicBoolean(false);

	public BufferPoolService(ServerSettings serverSettings) {
		this.serverSettings = serverSettings;

		poolSize = serverSettings.getR6BufferPoolSize();
		bufferSize = ServerConstant.MAX_CHUNK_SIZE; // serverSettings.getR6BufferSizeMB()*1000*1000;
		this.pool = new ArrayBlockingQueue<>(poolSize);
	}

	@PostConstruct
	protected synchronized void onInitialize() {

		setStatus(ServiceStatus.STARTING);

		startuplogger.debug("Started -> " + BufferPoolService.class.getSimpleName());

		if (this.serverSettings.getRedundancyLevel() != RedundancyLevel.RAID_6) {
			startuplogger.debug("Buffers are not required for " + this.serverSettings.getRedundancyLevel().getName() + " and will not be initialized.");
		} else {
			initialize();
			startuplogger.debug("Buffer pool. " + poolSize + " | Total size. " + NumberFormatter.formatFileSize(poolSize * bufferSize));
		}
		setStatus(ServiceStatus.RUNNING);
	}

	private synchronized void initialize() {

		if (initialized.get())
			return;

		for (int i = 0; i < poolSize; i++) {
			pool.add(new byte[bufferSize]);
		}
		initialized.set(true);
	}

	public byte[] acquire() {

		if (!initialized.get())
			initialize();

		try {
			return pool.take(); // back-pressure
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new IllegalStateException("Interrupted while acquiring buffer", e);
		}
	}

	public void release(byte[] buffer) {

		if (!initialized.get())
			initialize();

		if (buffer != null) {
			pool.offer(buffer);
		}
	}

	public int getPoolSize() {
		return poolSize;
	}

	public int getBufferSize() {
		return bufferSize;
	}

	public void setPoolSize(int poolSize) {
		this.poolSize = poolSize;
	}

	public void setBufferSize(int bufferSize) {
		this.bufferSize = bufferSize;
	}

}
