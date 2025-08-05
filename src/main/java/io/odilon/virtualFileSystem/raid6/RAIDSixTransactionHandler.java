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

import io.odilon.virtualFileSystem.model.VirtualFileSystemOperation;

/**
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 * 
 */
public class RAIDSixTransactionHandler extends RAIDSixHandler {

    public RAIDSixTransactionHandler(RAIDSixDriver driver) {
        super(driver);
    }

    protected void rollback(VirtualFileSystemOperation operation) {
        getDriver().rollback(operation, false);
    }

    protected void rollback(VirtualFileSystemOperation operation, boolean recoveryMode) {
        getDriver().rollback(operation, recoveryMode);
    }

    protected void rollback(VirtualFileSystemOperation operation, Object payload, boolean recoveryMode) {
        getDriver().rollback(operation, payload, recoveryMode);
    }

}
