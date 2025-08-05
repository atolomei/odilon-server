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

import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.Optional;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import io.odilon.log.Logger;
import io.odilon.model.RedundancyLevel;
import io.odilon.model.SharedConstant;
import io.odilon.virtualFileSystem.model.JournalService;
import io.odilon.virtualFileSystem.model.OperationCode;
import io.odilon.virtualFileSystem.model.VirtualFileSystemOperation;

/**
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
public class OdilonVirtualFileSystemOperation implements VirtualFileSystemOperation {

    static private Logger logger = Logger.getLogger(OdilonVirtualFileSystemOperation.class.getName());

    @JsonIgnore
    static private ObjectMapper mapper = new ObjectMapper();

    static {
        mapper.registerModule(new JavaTimeModule());
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        mapper.registerModule(new Jdk8Module());
    }

    @JsonIgnore
    static final private DateTimeFormatter formatter = DateTimeFormatter.ofPattern("YYYY-MM-dd'T'HH:mm:ss.XXX-z", Locale.ENGLISH);

    @JsonIgnore
    private JournalService journalService;

    @JsonProperty("id")
    private String id;

    @JsonProperty("version")
    private int version;

    @JsonProperty("bucketId")
    private Long bucketId;

    @JsonProperty("bucketName")
    private String bucketName;

    @JsonProperty("objectName")
    private String objectName;

    @JsonProperty("timestamp")
    private OffsetDateTime timestamp;

    @JsonProperty("operation")
    private OperationCode operationCode;

    @JsonProperty("raid")
    private RedundancyLevel raid;

    @JsonProperty("replicates")
    private boolean replicates;

    public OdilonVirtualFileSystemOperation() {
    }

    public OdilonVirtualFileSystemOperation(String id, OperationCode op, Optional<Long> bucketId, Optional<String> bucketName,
            Optional<String> objectName, Optional<Integer> iVersion, RedundancyLevel raid, JournalService journalService) {

        this.id = id;
        this.operationCode = op;

        if (iVersion.isPresent())
            version = iVersion.get().intValue();

        if (bucketId.isPresent())
            this.bucketId = bucketId.get();

        if (objectName.isPresent())
            this.objectName = objectName.get();

        if (bucketName.isPresent())
            this.bucketName = bucketName.get();

        this.raid = raid;
        this.journalService = journalService;
        this.timestamp = OffsetDateTime.now();
        this.replicates = op.isReplicates();
    }

    public boolean isReplicates() {
        return replicates;
    }

    public void setReplicates(boolean replicates) {
        this.replicates = replicates;
    }

    @Override
    public Long getBucketId() {
        return bucketId;
    }

    public void setBucketId(Long bucketId) {
        this.bucketId = bucketId;
    }

    @Override
    public String getObjectName() {
        return objectName;
    }

    public void setObjectName(String objectName) {
        this.objectName = objectName;
    }

    public int getVersion() {
        return this.version;
    }

    @Override
    public String getUUID() {
        return operationCode.getEntityGroupCode() + ":" + ((bucketId != null) ? bucketId.toString() : "null") + ":"
                + ((objectName != null) ? objectName : "null");
    }

    protected void setTimestamp(OffsetDateTime date) {
        this.timestamp = date;
    }

    protected void setRedundancyLevel(RedundancyLevel level) {
        this.raid = level;
    }

    @Override
    public RedundancyLevel getRedundancyLevel() {
        return this.raid;
    }

    @Override
    public boolean equals(Object o) {

        if (o == this) {
            return true;
        }

        if (o instanceof OdilonVirtualFileSystemOperation) {
            String oid = ((OdilonVirtualFileSystemOperation) o).getId();
            if (this.id == null)
                return oid == null;
            if (oid == null)
                return false;
            return this.id.equals(oid);
        }
        return false;

    }

    @Override
    public String toJSON() {
        try {
            return mapper.writeValueAsString(this);
        } catch (Exception e) {
            logger.error(e, SharedConstant.NOT_THROWN);
            return "\"error\":\"" + e.getClass().getName() + "\"";
        }
    }

    @Override
    public String toString() {
        StringBuilder str = new StringBuilder();
        str.append(this.getClass().getSimpleName());
        str.append(toJSON());
        return str.toString();
    }

    @Override
    public OffsetDateTime getTimestamp() {
        return timestamp;
    }

    @Override
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    @Override
    public OperationCode getOperationCode() {
        return operationCode;
    }

    public void setOp(OperationCode op) {
        this.operationCode = op;
    }

    @Override
    public boolean commit(Object payload) {
        return this.getJournalService().commit(this, payload);
    }

    @Override
    public boolean commit() {
        return this.getJournalService().commit(this);
    }

    @Override
    public boolean cancel() {
        return this.getJournalService().cancel(this);
    }

    @Override
    public boolean cancel(Object payload) {
        return this.getJournalService().cancel(this, payload);
    }

    public void setJournalService(JournalService journalService) {
        this.journalService = journalService;
    }

    public JournalService getJournalService() {
        return this.journalService;
    }

    @Override
    public String getBucketName() {
        return this.bucketName;
    }

    @Override
    public void setOperationCode(OperationCode code) {
        this.operationCode = code;
    }

}
