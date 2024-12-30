package io.odilon.virtualFileSystem.raid6;

import io.odilon.virtualFileSystem.model.VirtualFileSystemOperation;

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
