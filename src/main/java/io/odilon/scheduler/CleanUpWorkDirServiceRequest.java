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
import io.odilon.model.SharedConstant;
import io.odilon.virtualFileSystem.OdilonVFSperation;
import io.odilon.virtualFileSystem.model.VFSOperation;

/**
 * <p>
 * NOT USED YET
 * </p>
 * 
 * @see {@link RAIDZeroUpdateObjectHandler}, {@link RAIDOneUpdateObjectHandler}
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */

@Component
@Scope("prototype")
@JsonTypeName("cleanUpWorkDir")
public class CleanUpWorkDirServiceRequest extends AbstractServiceRequest implements StandardServiceRequest {

    static private Logger logger = Logger.getLogger(CleanUpWorkDirServiceRequest.class.getName());

    private static final long serialVersionUID = 1L;

    @JsonIgnore
    private boolean isSuccess = false;

    @JsonProperty("operation")
    private OdilonVFSperation operation;

    protected CleanUpWorkDirServiceRequest() {
    }

    public CleanUpWorkDirServiceRequest(VFSOperation operation) {
        this.operation = (OdilonVFSperation) operation;
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
            this.isSuccess = true;
            setStatus(ServiceRequestStatus.COMPLETED);

        } catch (Exception e) {
            this.isSuccess = false;
            logger.error(e, SharedConstant.NOT_THROWN);
        }
    }

    @Override
    public boolean isObjectOperation() {
        return false;
    }

    @Override
    public void stop() {
        this.isSuccess = true;
    }

    public VFSOperation getVFSOperation() {
        return this.operation;
    }

    @Override
    public boolean isSuccess() {
        return this.isSuccess;
    }

    @Override
    public String getUUID() {
        return this.operation.getUUID();
    }

}
