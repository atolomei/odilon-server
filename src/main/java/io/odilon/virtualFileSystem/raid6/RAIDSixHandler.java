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
package io.odilon.virtualFileSystem.raid6;

import javax.annotation.concurrent.ThreadSafe;

import io.odilon.virtualFileSystem.BaseRAIDHandler;
import io.odilon.virtualFileSystem.RAIDHandler;
import io.odilon.virtualFileSystem.model.VirtualFileSystemOperation;
import io.odilon.virtualFileSystem.model.VirtualFileSystemService;

/**
 * <p>
 * Base class for all RAID 6 hadler
 * </p>
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 * 
 * @see {@link RAIDSixCreateObjectHandler}
 * @see {@link RAIDSixUpdateObjectHandler}
 * @see {@link RAIDSixDeleteObjectHandler}
 * 
 */
@ThreadSafe
public abstract class RAIDSixHandler extends BaseRAIDHandler implements RAIDHandler {

    private final RAIDSixDriver driver;

    public RAIDSixHandler(RAIDSixDriver driver) {
        this.driver = driver;
    }

    @Override
    public RAIDSixDriver getDriver() {
        return this.driver;
    }

    public VirtualFileSystemService getVirtualFileSystemService() {
        return getDriver().getVirtualFileSystemService();
    }

    protected abstract void rollbackJournal(VirtualFileSystemOperation op, boolean recoveryMode);
}
