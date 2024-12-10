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
package io.odilon.virtualFileSystem.model;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import io.odilon.log.Logger;
import io.odilon.model.ObjectMetadata;
import io.odilon.model.ObjectStatus;
import io.odilon.model.SharedConstant;
import io.odilon.util.Check;

/**
 * <p>
 * Bucket iterator
 * </p>
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
public abstract class BucketIterator implements Iterator<Path> {

    private static final Logger logger = Logger.getLogger(BucketIterator.class.getName());

    static private ObjectMapper mapper = new ObjectMapper();

    static {
        mapper.registerModule(new JavaTimeModule());
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    @JsonProperty("cumulativeIndex")
    private long cumulativeIndex = 0;

    /** next item to return -> 0 .. [ list.size()-1 ] */
    @JsonIgnore
    private int relativeIndex = 0;

    @JsonProperty("prefix")
    private String prefix = null;

    @JsonProperty("agentId")
    private String agentId = null;

    @JsonProperty("offset")
    private Long offset = Long.valueOf(0);

    @JsonProperty("bucketId")
    private final Long bucketId;

    @JsonProperty("bucketName")
    private final String bucketName;

    @JsonIgnore
    private boolean initiated = false;

    @JsonIgnore
    private List<Path> buffer;

    @JsonIgnore
    private IODriver driver;

    @JsonIgnore
    private final ServerBucket bucket;

    /**
     * @param bucketName can not be null
     */
    public BucketIterator(final IODriver driver, final ServerBucket bucket) {

        Check.requireNonNullArgument(driver, "driver is null");
        Check.requireNonNullArgument(bucket, "bucket is null");

        setDriver(driver);

        this.bucket = bucket;
        this.bucketId = bucket.getId();
        this.bucketName = bucket.getName();
    }

    abstract protected boolean fetch();

    abstract protected void init();

    @Override
    public synchronized boolean hasNext() {

        if (!isInitiated()) {
            init();
            return fetch();
        }
        /**
         * if the buffer still has items
         **/
        if (getRelativeIndex() < getBuffer().size())
            return true;

        return fetch();
    }

    @Override
    public synchronized Path next() {
        /**
         * if the buffer still has items to return
         */
        if (getRelativeIndex() < getBuffer().size()) {
            Path object = getBuffer().get(getRelativeIndex());
            setRelativeIndex(getRelativeIndex() + 1);
            incCumulativeIndex();
            return object;
        }
        boolean hasItems = fetch();
        if (!hasItems)
            throw new IndexOutOfBoundsException("No more items available. hasNext() should be called before this method."
                    + "[returned so far -> " + String.valueOf(getCumulativeIndex()) + "]");
        Path object = getBuffer().get(getRelativeIndex());
        incRelativeIndex();
        incCumulativeIndex();
        return object;
    }

    public void close() throws IOException {
    }

    public String getAgentId() {
        return agentId;
    }

    public void setAgentId(String agentId) {
        this.agentId = agentId;
    }

    public ServerBucket getBucket() {
        return this.bucket;
    }

    public Long getBucketId() {
        return bucket.getId();
    }

    public Long getOffset() {
        return offset;
    }

    public Long setOffset(Long offset) {
        this.offset = offset;
        return offset;
    }

    @Override
    public String toString() {
        StringBuilder str = new StringBuilder();
        str.append(this.getClass().getSimpleName() + "{");
        str.append(toJSON());
        str.append("}");
        return str.toString();
    }

    public String toJSON() {
        try {
            return mapper.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            logger.error(e, SharedConstant.NOT_THROWN);
            return "\"error\":\"" + e.getClass().getName() + " | " + e.getMessage() + "\"";
        }
    }

    protected void setDriver(IODriver driver) {
        this.driver = driver;
    }

    protected IODriver getDriver() {
        return driver;
    }

    /**
     * We are not using this filter (normally ObjectState.ENABLED)
     * 
     * @param path
     * @return
     */
    protected boolean isValidState(Path path) {
        return true;
    }

    /**
     * <p>
     * This method should be used when the delete operation is logical
     * (ObjectStatus.DELETED)
     * </p>
     * 
     * @param path
     * @return
     */
    protected boolean isObjectStateEnabled(Path path) {
        ObjectMetadata meta = getDriver().getObjectMetadata(getBucket(), path.toFile().getName());
        if (meta == null)
            return false;
        if (meta.getStatus() == ObjectStatus.ENABLED)
            return true;
        return false;
    }

    protected boolean isInitiated() {
        return initiated;
    }

    protected void setInitiated(boolean initiated) {
        this.initiated = initiated;
    }

    protected List<Path> getBuffer() {
        return buffer;
    }

    protected void setBuffer(List<Path> buffer) {
        this.buffer = buffer;
    }

    protected String getPrefix() {
        return prefix;
    }

    protected void setPrefix(String prefix) {
        this.prefix = prefix;
    }

    protected int getRelativeIndex() {
        return relativeIndex;
    }

    protected void incRelativeIndex() {
        this.relativeIndex++;
    }

    protected void setRelativeIndex(int relativeIndex) {
        this.relativeIndex = relativeIndex;
    }

    protected void incCumulativeIndex() {
        this.cumulativeIndex++;
    }

    protected long getCumulativeIndex() {
        return cumulativeIndex;
    }

    protected void setCumulativeIndex(long cumulativeIndex) {
        this.cumulativeIndex = cumulativeIndex;
    }
}
