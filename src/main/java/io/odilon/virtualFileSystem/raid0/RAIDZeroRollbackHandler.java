package io.odilon.virtualFileSystem.raid0;

import io.odilon.virtualFileSystem.model.VirtualFileSystemOperation;

public abstract class RAIDZeroRollbackHandler extends RAIDZeroHandler {

    final private VirtualFileSystemOperation operation;
    final private boolean recoveryMode;

    public RAIDZeroRollbackHandler(RAIDZeroDriver driver, VirtualFileSystemOperation operation, boolean recoveryMode) {
        super(driver);

        this.operation = operation;
        this.recoveryMode = recoveryMode;
    }

    protected VirtualFileSystemOperation getOperation() {
        return this.operation;
    }

    protected boolean isRecovery() {
        return this.recoveryMode;
    }

    protected abstract void rollback();
}
