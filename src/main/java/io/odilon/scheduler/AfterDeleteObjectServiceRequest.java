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
package io.odilon.scheduler;

import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

import io.odilon.log.Logger;
import io.odilon.model.ObjectMetadata;
import io.odilon.model.SharedConstant;
import io.odilon.virtualFileSystem.model.VFSOp;
import io.odilon.virtualFileSystem.model.VirtualFileSystemService;

/**
 * <p>
 * ServiceRequest executed Async after a {@code VFSop.DELETE_OBJECT} or
 * {@code VFSop.DELETE_OBJECT_PREVIOUS_VERSIONS}
 * </p>
 * 
 * <b>VFSop.DELETE_OBJECT</b>
 * <p>
 * Cleans up all previous versions of an {@link VirtualFileSystemObject}
 * (ObjectMetadata and Data).<br/>
 * Delete backup directory. <br/>
 * This request is executed Async after the delete transaction commited.
 * </p>
 * 
 * <b>VFSop.DELETE_OBJECT_PREVIOUS_VERSIONS</b>
 * <p>
 * Cleans up all previous versions of an {@link VirtualFileSystemObject}
 * (ObjectMetadata and Data), but keeps the head version.<br/>
 * Delete backup directory. <br/>
 * This request is executed Async after the delete transaction commited.
 * </p>
 *
 * <b>RETRIES</b>
 * <p>
 * if the request can not complete due to serious system issue, the request is
 * discarded after 5 attemps. The clean up process will be executed after next
 * system startup
 * </p>
 * 
 * * @see {@link RAIDZeroDeleteObjectHandler},
 * {@link RAIDOneDeleteObjectHandler}
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
@Component
@Scope("prototype")
@JsonTypeName("afterDeleteObject")
public class AfterDeleteObjectServiceRequest extends AbstractServiceRequest implements StandardServiceRequest {

    static private Logger logger = Logger.getLogger(AfterDeleteObjectServiceRequest.class.getName());

    private static final long serialVersionUID = 1L;

    @JsonProperty("meta")
    ObjectMetadata meta;

    @JsonProperty("headVersion")
    int headVersion = 0;

    @JsonProperty("vfsop")
    VFSOp vfsop;

    @JsonIgnore
    private boolean isSuccess = false;

    /**
     * <p>
     * created by the RAIDZeroDriver
     * </p>
     */
    protected AfterDeleteObjectServiceRequest() {
    }

    public AfterDeleteObjectServiceRequest(VFSOp vfsop, ObjectMetadata meta, int headVersion) {

        this.vfsop = vfsop;
        this.meta = meta;
        this.headVersion = headVersion;
    }

    @Override
    public boolean isSuccess() {
        return this.isSuccess;
    }

    /**
     * <p>
     * {@link ServiceRequestExecutor} closes the Request
     * </p>
     */
    @Override
    public void execute() {
        try {
            setStatus(ServiceRequestStatus.RUNNING);
            clean();
            this.isSuccess = true;
            setStatus(ServiceRequestStatus.COMPLETED);

        } catch (Exception e) {
            logger.error(e, SharedConstant.NOT_THROWN);
            this.isSuccess = false;
            setStatus(ServiceRequestStatus.ERROR);
        }
    }

    @Override
    public String getUUID() {
        if (meta == null)
            return "null";
        return ((meta.bucketId != null) ? meta.bucketId.toString() : "null") + ":"
                + ((meta.objectName != null) ? meta.objectName : "null");
    }

    @Override
    public boolean isObjectOperation() {
        return true;
    }

    @Override
    public void stop() {
        this.isSuccess = true;
    }

    /**
     * <p>
     * There is nothing to do if the VFSOp or ObjectMetadata are null at this point.
     * They should never have reached here
     * </p>
     * 
     */
    private void clean() {

        VirtualFileSystemService vfs = getApplicationContext().getBean(VirtualFileSystemService.class);

        if (this.vfsop == null) {
            logger.error("Invalid " + VFSOp.class.getName() + " is null ", SharedConstant.NOT_THROWN);
            return;
        }

        if (this.meta == null) {
            logger.error("Invalid " + ObjectMetadata.class.getName() + " is null ", SharedConstant.NOT_THROWN);
            return;
        }

        if (this.vfsop == VFSOp.DELETE_OBJECT)
            vfs.createVFSIODriver().postObjectDeleteTransaction(meta, headVersion);

        else if (this.vfsop == VFSOp.DELETE_OBJECT_PREVIOUS_VERSIONS)
            vfs.createVFSIODriver().postObjectPreviousVersionDeleteAllTransaction(meta, headVersion);
        else
            logger.error("Invalid " + VFSOp.class.getName() + " -> " + this.vfsop.getName(), SharedConstant.NOT_THROWN);
    }

}
