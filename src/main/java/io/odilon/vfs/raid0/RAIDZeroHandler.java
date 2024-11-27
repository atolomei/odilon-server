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

import io.odilon.vfs.BaseRAIDHandler;
import io.odilon.vfs.RAIDHandler;
import io.odilon.vfs.model.Drive;
import io.odilon.vfs.model.ServerBucket;
import io.odilon.vfs.model.VFSOperation;
import io.odilon.vfs.model.VirtualFileSystemService;

/**
 * <p>Base class for {@link RAIDZeroDriver} operations</p>
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
@ThreadSafe
public abstract class RAIDZeroHandler extends BaseRAIDHandler implements RAIDHandler {

	private final RAIDZeroDriver driver;
	
	public RAIDZeroHandler(RAIDZeroDriver driver) {
		this.driver=driver;
	}

	@Override
	public RAIDZeroDriver getDriver() {
		return this.driver;
	}
	
	public VirtualFileSystemService getVirtualFileSystemService() {
		return getDriver().getVirtualFileSystemService();
	}
	
	public Drive getWriteDrive(ServerBucket bucket, String objectName) {
		return getDriver().getWriteDrive(bucket, objectName);
	}
	
	protected abstract void rollbackJournal(VFSOperation op, boolean recoveryMode);
}
