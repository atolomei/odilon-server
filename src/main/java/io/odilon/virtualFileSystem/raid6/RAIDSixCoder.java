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

import com.fasterxml.jackson.annotation.JsonIgnore;

import io.odilon.cache.FileCacheService;
import io.odilon.model.BaseObject;
import io.odilon.model.ObjectMetadata;
import io.odilon.util.Check;
import io.odilon.virtualFileSystem.model.ServerBucket;
import io.odilon.virtualFileSystem.model.VirtualFileSystemService;

/**
 * 
 * @see RAIDSixDecoder
 * @see RAIDSixEncoder
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
public class RAIDSixCoder extends BaseObject {

    @JsonIgnore
    private final RAIDSixDriver driver;

    protected RAIDSixCoder(RAIDSixDriver driver) {
        Check.requireNonNull(driver);
        this.driver = driver;
    }

    public RAIDSixDriver getDriver() {
        return this.driver;
    }

    protected VirtualFileSystemService getVirtualFileSystemService() {
        return getDriver().getVirtualFileSystemService();
    }

    protected String objectInfo(ObjectMetadata meta) {
        return getDriver().objectInfo(meta);
    }

    protected FileCacheService getFileCacheService() {
        return getVirtualFileSystemService().getFileCacheService();
    }

    public String objectInfo(ServerBucket bucket, String objectName, String fileName) {
        return getDriver().objectInfo(bucket, objectName, fileName);
    }

    public String objectInfo(ServerBucket bucket, String objectName) {
        return getDriver().objectInfo(bucket, objectName);
    }

}
