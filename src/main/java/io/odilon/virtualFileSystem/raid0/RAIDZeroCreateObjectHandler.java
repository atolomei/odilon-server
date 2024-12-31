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
package io.odilon.virtualFileSystem.raid0;

import java.io.InputStream;
import java.util.List;
import java.util.Optional;

import javax.annotation.concurrent.ThreadSafe;

import org.checkerframework.checker.nullness.qual.NonNull;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.odilon.errors.InternalCriticalException;
import io.odilon.log.Logger;
import io.odilon.model.SharedConstant;
import io.odilon.virtualFileSystem.model.ServerBucket;
import io.odilon.virtualFileSystem.model.OperationCode;
import io.odilon.virtualFileSystem.model.VirtualFileSystemOperation;

/**
 * <p>
 * RAID 0 Handler <br/>
 * Creates new Objects ({@link OperationCode.CREATE_OBJECT})
 * </p>
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
@ThreadSafe
public class RAIDZeroCreateObjectHandler extends RAIDZeroTransactionHandler {

    private static Logger logger = Logger.getLogger(RAIDZeroCreateObjectHandler.class.getName());

    @JsonProperty("bucket")
    private final ServerBucket bucket;

    @JsonProperty("objectName")
    private final String objectName;

    /**
     * <p>
     * Created and used only from {@link RAIDZeroDriver}
     * </p>
     */
    protected RAIDZeroCreateObjectHandler(RAIDZeroDriver driver, ServerBucket bucket, String objectName) {
        super(driver);
        this.bucket = bucket;
        this.objectName = objectName;
    }

    /**
     * <p>
     * The procedure is the same whether version control is enabled or not
     * </p>
     * 
     * @param bucket
     * @param objectName
     * @param stream
     * @param srcFileName
     * @param contentType
     * @param customTags
     */
    protected void create(InputStream stream, String srcFileName, String contentType, Optional<List<String>> customTags) {

        VirtualFileSystemOperation operation = null;
        boolean commitOk = false;
        boolean isMainException = false;

        objectWriteLock(getBucket(), getObjectName());
        try {

            bucketReadLock(getBucket());
            try (stream) {

                /** must be executed inside the critical zone. */
                checkExistsBucket(getBucket());

                /** must be executed inside the critical zone. */
                checkNotExistObject(getBucket(), getObjectName());

                int version = 0;
                operation = createObject();

                saveData(stream, srcFileName);
                saveMetadata(srcFileName, contentType, version, customTags);

                commitOk = operation.commit();

            } catch (InternalCriticalException e1) {
                commitOk = false;
                isMainException = true;
                throw e1;
            } catch (Exception e) {
                commitOk = false;
                isMainException = true;
                throw new InternalCriticalException(e, info(srcFileName));
            } finally {
                try {
                    if (!commitOk) {
                        try {
                            rollback(operation);
                        } catch (InternalCriticalException e) {
                            if (!isMainException)
                                throw e;
                            else
                                logger.error(e, info(srcFileName), SharedConstant.NOT_THROWN);
                        } catch (Exception e) {
                            if (!isMainException)
                                throw new InternalCriticalException(e, info(srcFileName));
                            else
                                logger.error(e, info(srcFileName), SharedConstant.NOT_THROWN);
                        }
                    }
                } finally {
                    bucketReadUnLock(getBucket());
                }
            }
        } finally {
            objectWriteUnLock(getBucket(), getObjectName());
        }
    }

    private ServerBucket getBucket() {
        return bucket;
    }

    private String getObjectName() {
        return objectName;
    }

    private VirtualFileSystemOperation createObject() {
        return createObject(getBucket(), getObjectName());
    }

    private void saveMetadata(String srcFileName, String contentType, int version, Optional<List<String>> customTags) {
        saveMetadata(getBucket(), getObjectName(), srcFileName, contentType, version, customTags);
    }

    private void saveData(@NonNull InputStream stream, String srcFileName) {
        saveData(getBucket(), getObjectName(), stream, srcFileName);
    }

    private String info(String str) {
        return objectInfo(getBucket(), getObjectName(), str);
    }
}
