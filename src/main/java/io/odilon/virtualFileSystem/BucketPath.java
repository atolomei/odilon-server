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

package io.odilon.virtualFileSystem;

import java.nio.file.Path;
import java.nio.file.Paths;

import io.odilon.model.ServerConstant;
import io.odilon.virtualFileSystem.model.Drive;
import io.odilon.virtualFileSystem.model.ServerBucket;

public class BucketPath extends PathBuilder {
    
    static final String BUCKET_METADATA = "bucketmetadata-";
    
    
    private final Drive drive;
    
    private final ServerBucket bucket;
    
    public BucketPath(Drive drive, ServerBucket bucket) {
        this.drive=drive;
        this.bucket=bucket;
    }

    
    public Path cacheDirPath() {
        return null;
    }
    
    public Path workDirPath() {
        return Paths.get(getDrive().getBucketWorkDirPath(getBucket()));
    }
    
    
    public Path bucketMetadata(Context context) {
        if (context==Context.BACKUP)
            return workDirPath().resolve(BUCKET_METADATA + getBucket().getId().toString() + ServerConstant.JSON);
        
        throw new RuntimeException("not done");
        
    }
    
    
    private Drive getDrive() {
        return drive;
    }


    private ServerBucket getBucket() {
        return bucket;
    }


    public Path dataDirPath() {
        return null;
    }
    
    public Path metadataDirPath() {
        return null;
    }
    
}

