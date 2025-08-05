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

import com.fasterxml.jackson.annotation.JsonProperty;

import io.odilon.virtualFileSystem.model.VirtualFileSystemOperation;

/**
 * <p>
 * Callers must ensure proper concurrency control before calling
 * </p>
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
public abstract class RAIDSixRollbackHandler extends RAIDSixHandler {

    @JsonProperty("operation")
    final private VirtualFileSystemOperation operation;

    @JsonProperty("recovery")
    final private boolean recovery;

    public RAIDSixRollbackHandler(RAIDSixDriver driver, VirtualFileSystemOperation operation, boolean recovery) {
        super(driver);
        this.operation = operation;
        this.recovery = recovery;
    }

    protected VirtualFileSystemOperation getOperation() {
        return this.operation;
    }

    protected boolean isRecovery() {
        return this.recovery;
    }

    protected abstract void rollback();
}
