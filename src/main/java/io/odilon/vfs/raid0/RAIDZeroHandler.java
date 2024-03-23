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

package io.odilon.vfs.raid0;

import javax.annotation.concurrent.ThreadSafe;

import io.odilon.model.RedundancyLevel;
import io.odilon.vfs.RAIDHandler;
import io.odilon.vfs.model.Drive;
import io.odilon.vfs.model.JournalService;
import io.odilon.vfs.model.LockService;
import io.odilon.vfs.model.VFSOperation;
import io.odilon.vfs.model.VirtualFileSystemService;

/**
 * <p>Base class for {@link RAIDZeroDriver} operations</p>
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */

@ThreadSafe
public abstract class RAIDZeroHandler implements RAIDHandler {

	private final RAIDZeroDriver driver;
	
	public RAIDZeroHandler(RAIDZeroDriver driver) {
		this.driver=driver;
	}

	public RAIDZeroDriver getDriver() {
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
	
	public Drive getWriteDrive(String bucketName, String objectName) {
		return this.driver.getWriteDrive(bucketName, objectName);
	}
	

}
