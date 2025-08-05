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

package io.odilon.virtualFileSystem.model;

import java.time.OffsetDateTime;

import io.odilon.model.RedundancyLevel;

/**
 * <p>
 * information processing that is divided into individual, indivisible
 * operations called transactions. Each transaction must succeed or fail as a
 * complete unit; it can never be only partially complete. They are persisted on
 * the {@link VirtualFileSystem} by the {@link JournalService}.
 * 
 * </p>
 * 
 * @see {@link OperationCode}
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
public interface VirtualFileSystemOperation {

    public OperationCode getOperationCode();

    public boolean commit();

    public boolean commit(Object payload);

    public boolean cancel();

    public boolean cancel(Object payload);

    public boolean isReplicates();

    public String getId();

    public String toJSON();

    public OffsetDateTime getTimestamp();

    public RedundancyLevel getRedundancyLevel();

    public String getObjectName();

    public String getBucketName();

    public Long getBucketId();

    public int getVersion();

    public String getUUID();

    public void setOperationCode(OperationCode code);

    public void setBucketId(Long bucketId);

    public void setObjectName(String name);

}
