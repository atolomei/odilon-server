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
package io.odilon.virtualFileSystem.raid0;

import java.io.InputStream;
import java.util.List;
import java.util.Optional;

import javax.annotation.concurrent.ThreadSafe;

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
public class RAIDZeroCreateObjectHandler extends RAIDZeroTransactionObjectHandler {

    private static Logger logger = Logger.getLogger(RAIDZeroCreateObjectHandler.class.getName());

    /**
     * <p>
     * Created and used only from {@link RAIDZeroDriver}
     * </p>
     */
    protected RAIDZeroCreateObjectHandler(RAIDZeroDriver driver, ServerBucket bucket, String objectName) {
        super(driver, bucket, objectName);
    }

    /**
     * <p>
     * The procedure is the same whether version control is enabled or not
     * </p>
     * 
     * @param bucket
     * @param objectName
     * @param stream
     * @param sourceFileName
     * @param contentType
     * @param customTags
     */
    protected void create(InputStream stream, String sourceFileName, String contentType, Optional<List<String>> customTags) {

        VirtualFileSystemOperation operation = null;
        boolean commitOk = false;
        boolean isMainException = false;

        objectWriteLock();
        try {

            bucketReadLock();
            try (stream) {

                checkExistsBucket();
                checkNotExistObject();

                /** start operation */
                operation = createObject();

                /** save (metadata and data) */
                save(stream, sourceFileName, contentType, customTags);

                /** commit */
                commitOk = operation.commit();

            } catch (InternalCriticalException e1) {
                isMainException = true;
                throw e1;
            } catch (Exception e2) {
                isMainException = true;
                throw new InternalCriticalException(e2, info(sourceFileName));
            } finally {
                try {
                    if (!commitOk) {
                        try {
                            rollback(operation);
                        } catch (InternalCriticalException e) {
                            if (!isMainException)
                                throw e;
                            else
                                logger.error(e, info(sourceFileName), SharedConstant.NOT_THROWN);
                        } catch (Exception e) {
                            if (!isMainException)
                                throw new InternalCriticalException(e, info(sourceFileName));
                            else
                                logger.error(e, info(sourceFileName), SharedConstant.NOT_THROWN);
                        }
                    }
                } finally {
                    bucketReadUnLock();
                }
            }
        } finally {
            objectWriteUnLock();
        }
    }

    private VirtualFileSystemOperation createObject() {
        return createObject(getBucket(), getObjectName());
    }

    private void save(InputStream stream, String srcFileName, String contentType, Optional<List<String>> customTags) {
        saveData(getBucket(), getObjectName(), stream, srcFileName);
        saveMetadata(getBucket(), getObjectName(), srcFileName, contentType, VERSION_ZERO, customTags);
    }

}
