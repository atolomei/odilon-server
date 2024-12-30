package io.odilon.virtualFileSystem.raid0;

import org.apache.commons.io.FileUtils;

import io.odilon.errors.InternalCriticalException;
import io.odilon.log.Logger;
import io.odilon.model.SharedConstant;
import io.odilon.virtualFileSystem.ObjectPath;

import io.odilon.virtualFileSystem.model.ServerBucket;
import io.odilon.virtualFileSystem.model.VirtualFileSystemOperation;

public class RAIDZeroRollbackCreateHandler extends RAIDZeroRollbackHandler {
        
    private static Logger logger = Logger.getLogger(RAIDZeroRollbackCreateHandler.class.getName());

    
    public RAIDZeroRollbackCreateHandler(RAIDZeroDriver driver, VirtualFileSystemOperation operation, boolean recoveryMode) {
        super(driver, operation, recoveryMode);
    }
    
    @Override
    protected void rollback() {
        
        if (getOperation() == null)
            return;
               
        boolean rollbackOK = false;
        
        try {
            
            ServerBucket bucket = getBucketCache().get(getOperation().getBucketId());
            String objectName = getOperation().getObjectName();
            
            if (isStandByEnabled())
                getReplicationService().cancel(getOperation());

            ObjectPath path = new ObjectPath(getWriteDrive(bucket, objectName), bucket, objectName);

            FileUtils.deleteQuietly(path.metadataDirPath().toFile());
            FileUtils.deleteQuietly(path.dataFilePath().toFile());

            rollbackOK = true;

        } catch (InternalCriticalException e) {
            if (!isRecovery())
                throw (e);
            else
                logger.error(e, opInfo(getOperation()), SharedConstant.NOT_THROWN);
        } catch (Exception e) {
            if (!isRecovery())
                throw new InternalCriticalException(e, opInfo(getOperation()));
            else
                logger.error(e, opInfo(getOperation()), SharedConstant.NOT_THROWN);
        } finally {
            if (rollbackOK || isRecovery())
                getOperation().cancel();
        }
    }

}
