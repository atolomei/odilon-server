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

import java.io.InputStream;
import java.util.List;

import io.odilon.model.ObjectMetadata;
import io.odilon.virtualFileSystem.model.Drive;
import io.odilon.virtualFileSystem.model.DriveStatus;
import io.odilon.virtualFileSystem.model.ServerBucket;

/**
 * <p>
 * RS Encoder for drive Sync process.
 * </p>
 * <p>
 * The difference between this class and the standard {@link RAIDSixEncoder} is
 * that this one only saves the RS Blocks that go to the newly added disks (ie.
 * blocks on enabled Drives are <b>not</b> touched).
 * </p>
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
public class RAIDSixSDriveSyncEncoder extends RAIDSixEncoder {

    /**
     * @param driver can not be null
     */
    protected RAIDSixSDriveSyncEncoder(RAIDSixDriver driver) {
        super(driver);
    }

    protected RAIDSixSDriveSyncEncoder(RAIDSixDriver driver, List<Drive> zDrives) {
        super(driver, zDrives);
    }

    /**
     * <p>
     * We can not use the {@link ObjectMetadata} here because it may not exist yet.
     * The steps to upload objects are: <br/>
     * - upload binary data <br/>
     * - create ObjectMetadata <br/>
     * </p>
     */
    public RAIDSixBlocks encodeHead(InputStream is, ServerBucket bucket, String objectName) {
        return super.encodeHead(is, bucket, objectName);
    }

    /**
     * <p>
     * We can not use the {@link ObjectMetadata} here because it may not exist yet.
     * The steps to upload objects are: <br/>
     * - upload binary data <br/>
     * - create ObjectMetadata <br/>
     * </p>
     */

    public RAIDSixBlocks encodeVersion(InputStream is, ServerBucket bucket, String objectName, int version) {
        return super.encodeVersion(is, bucket, objectName, version);
    }

    /**
     * <p>
     * only write on Drives in status {@link DriveStatus.NOTSYNC}
     * </p>
     */
    protected boolean isWrite(int diskOrder) {
        return (getDrives().get(diskOrder).getDriveInfo().getStatus() == DriveStatus.NOTSYNC);
    }

}
