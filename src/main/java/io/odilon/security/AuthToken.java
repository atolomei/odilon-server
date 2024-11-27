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
package io.odilon.security;

import java.io.Serializable;
import java.time.OffsetDateTime;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.odilon.model.ServerConstant;
import io.odilon.model.BaseObject;

/**
 * <p>
 * AuthToken are used to generate timed pre-signed urls to access objects
 * without authentication
 * </p>
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
public class AuthToken extends BaseObject implements Serializable {

    static final int VERSION = 1;

    private static final long serialVersionUID = 1L;

    @JsonProperty("bucketName")
    public String bucketName;

    @JsonProperty("objectName")
    public String objectName;

    @JsonProperty("expirationDate")
    public OffsetDateTime expirationDate;

    @JsonProperty("keyVersion")
    public int keyVersion;

    public AuthToken() {
    }

    public AuthToken(String bucketName, String objectName) {
        this(bucketName, objectName, OffsetDateTime.now().plusSeconds(ServerConstant.DEFAULT_EXPIRY_TIME), VERSION);
    }

    public AuthToken(String bucketName, String objectName, int durationSeconds) {
        this(bucketName, objectName, (durationSeconds > 0) ? OffsetDateTime.now().plusSeconds(durationSeconds)
                : OffsetDateTime.now().plusSeconds(ServerConstant.DEFAULT_EXPIRY_TIME), VERSION);
    }

    public AuthToken(String bucketName, String objectName, OffsetDateTime expirationDate, int keyVersion) {
        this.bucketName = bucketName;
        this.objectName = objectName;
        this.expirationDate = OffsetDateTime.now().plusDays(1);
        this.keyVersion = keyVersion;
    }

    /**
     * <p>
     * Returns bucket name
     * </p>
     */
    public String bucketName() {
        return bucketName;
    }

    /**
     * <p>
     * Returns object name
     * </p>
     */
    public String name() {
        return objectName;
    }

    public OffsetDateTime expirationDate() {
        return expirationDate;
    }

    public boolean isValid() {
        return expirationDate.isAfter(OffsetDateTime.now());
    }

    public String getBucketName() {
        return bucketName;
    }

    public void setBucketName(String bucketName) {
        this.bucketName = bucketName;
    }

    public String getObjectName() {
        return objectName;
    }

    public void setObjectName(String objectName) {
        this.objectName = objectName;
    }

    public OffsetDateTime getExpirationDate() {
        return expirationDate;
    }

    public void setExpirationDate(OffsetDateTime expirationDate) {
        this.expirationDate = expirationDate;
    }

    public int getKeyVersion() {
        return keyVersion;
    }

    public void setKeyVersion(int keyVersion) {
        this.keyVersion = keyVersion;
    }

}
