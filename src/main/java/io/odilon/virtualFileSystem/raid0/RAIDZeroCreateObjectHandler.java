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

    /**
     * <p>
     * Created and used only from {@link RAIDZeroDriver}
     * </p>
     */
    protected RAIDZeroCreateObjectHandler(RAIDZeroDriver driver) {
        super(driver);
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
    protected void create(@NonNull ServerBucket bucket, @NonNull String objectName, @NonNull InputStream stream, String srcFileName,
            String contentType, Optional<List<String>> customTags) {

        VirtualFileSystemOperation operation = null;
        boolean commitOk = false;
        boolean isMainException = false;

        objectWriteLock(bucket, objectName);
        try {

            bucketReadLock(bucket);
            try (stream) {

                /** must be executed inside the critical zone. */
                checkExistsBucket(bucket);

                /** must be executed inside the critical zone. */
                checkNotExistObject(bucket, objectName);

                int version = 0;
                operation = createObject(bucket, objectName);

                saveData(bucket, objectName, stream, srcFileName);
                saveMetadata(bucket, objectName, srcFileName, contentType, version, customTags);

                commitOk = operation.commit();

            } catch (InternalCriticalException e1) {
                commitOk = false;
                isMainException = true;
                throw e1;
            } catch (Exception e) {
                commitOk = false;
                isMainException = true;
                throw new InternalCriticalException(e, objectInfo(bucket, objectName, srcFileName));
            } finally {
                try {
                    if (!commitOk) {
                        try {
                            rollback(operation);
                        } catch (InternalCriticalException e) {
                            if (!isMainException)
                                throw e;
                            else
                                logger.error(e, objectInfo(bucket, objectName, srcFileName), SharedConstant.NOT_THROWN);
                        } catch (Exception e) {
                            if (!isMainException)
                                throw new InternalCriticalException(e, objectInfo(bucket, objectName, srcFileName));
                            else
                                logger.error(e, objectInfo(bucket, objectName, srcFileName), SharedConstant.NOT_THROWN);
                        }
                    }
                } finally {
                    bucketReadUnLock(bucket);
                }
            }
        } finally {
            objectWriteUnLock(bucket, objectName);
        }
    }
}
