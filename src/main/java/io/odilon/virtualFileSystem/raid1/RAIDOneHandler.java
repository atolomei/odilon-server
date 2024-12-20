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
package io.odilon.virtualFileSystem.raid1;

import javax.annotation.concurrent.ThreadSafe;

import io.odilon.model.RedundancyLevel;
import io.odilon.virtualFileSystem.BaseRAIDHandler;
import io.odilon.virtualFileSystem.RAIDHandler;
import io.odilon.virtualFileSystem.model.JournalService;
import io.odilon.virtualFileSystem.model.LockService;
import io.odilon.virtualFileSystem.model.VirtualFileSystemOperation;

/**
 * <p>Base class for all RAID 1 handlers</p>
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
@ThreadSafe
public abstract class RAIDOneHandler extends BaseRAIDHandler implements RAIDHandler {
	
	private final RAIDOneDriver driver;
	
	protected abstract void rollbackJournal(VirtualFileSystemOperation op, boolean recoveryMode);
	
	
	public RAIDOneHandler(RAIDOneDriver driver) {
		this.driver=driver;
	}

	public RAIDOneDriver getDriver() {
		return this.driver;
	}

	public JournalService getJournalService() {
		return this.driver.getJournalService();
	}

	public LockService getLockService() {
		return this.driver.getLockService();
	}
	
	protected boolean isEncrypt() {
		return this.driver.isEncrypt();
	}
	
	public RedundancyLevel getRedundancyLevel() {
		return this.driver.getRedundancyLevel(); 
	}
}
