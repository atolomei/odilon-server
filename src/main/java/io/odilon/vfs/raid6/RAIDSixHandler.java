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


package io.odilon.vfs.raid6;

import javax.annotation.concurrent.ThreadSafe;

import io.odilon.model.RedundancyLevel;
import io.odilon.vfs.RAIDHandler;
import io.odilon.vfs.model.JournalService;
import io.odilon.vfs.model.LockService;
import io.odilon.vfs.model.VFSOperation;
import io.odilon.vfs.model.VirtualFileSystemService;

/**
 * <p>Base class for all RAID 6 hadler </p>
 * @author atolomei@novamens.com (Alejandro Tolomei)
 * 
 * @see {@link RAIDSixCreateObjectHandler}
 * @see {@link RAIDSixUpdateObjectHandler}
 * @see {@link RAIDSixDeleteObjectHandler}
 * 
 */

@ThreadSafe
public abstract class RAIDSixHandler implements RAIDHandler {

	private final RAIDSixDriver driver;
	
	public RAIDSixHandler(RAIDSixDriver driver) {
		this.driver=driver;
	}

	public RAIDSixDriver getDriver() {
		return this.driver;
	}
	
	public VirtualFileSystemService getVFS() {
		return this.driver.getVFS();
	}
	
	public abstract void rollbackJournal(VFSOperation op, boolean recoveryMode);

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
