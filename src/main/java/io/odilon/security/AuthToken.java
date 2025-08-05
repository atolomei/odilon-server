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
package io.odilon.security;

import java.io.Serializable;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.odilon.model.ServerConstant;
import io.odilon.log.Logger;
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

	@SuppressWarnings("unused")
	static private Logger logger = Logger.getLogger(AuthToken.class.getName());

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

	@JsonProperty("objectCacheDurationSecs")
	public int objectCacheDurationSecs = 0;

	
	private static final DateTimeFormatter HTTP_DATE = DateTimeFormatter.RFC_1123_DATE_TIME;

	  
	public AuthToken() {
	}

	public AuthToken(String bucketName, String objectName) {
		this(bucketName, objectName, OffsetDateTime.now().plusSeconds(ServerConstant.DEFAULT_EXPIRY_TIME), VERSION, 0);
	}

	public AuthToken(String bucketName, String objectName, int durationSeconds) {
		this(bucketName, objectName, (durationSeconds > 0) ? OffsetDateTime.now().plusSeconds(durationSeconds)
				: OffsetDateTime.now().plusSeconds(ServerConstant.DEFAULT_EXPIRY_TIME), VERSION, 0);
	}

	public AuthToken(String bucketName, String objectName, OffsetDateTime expirationDate, int keyVersion, int objectCacheDurationSecs) {
		this.bucketName = bucketName;
		this.objectName = objectName;
		this.expirationDate = expirationDate;
		this.keyVersion = keyVersion;
		this.objectCacheDurationSecs=objectCacheDurationSecs;
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
	
	@JsonProperty("expirationDateString")
	public String expirationDateString() {
		if (this.expirationDate==null)
			return null;
		return HTTP_DATE.format(expirationDate);
	}
	
	@Override
	public String toString() {
	        StringBuilder str = new StringBuilder();
	        str.append(this.getClass().getSimpleName());
	        str.append(toJSON());
	        return str.toString();
	}

	public int getObjectCacheDurationSecs() {
		 return objectCacheDurationSecs;
	}

	public void setObjectCacheDurationSecs(int objectCacheDurationSecs) {
		this.objectCacheDurationSecs = objectCacheDurationSecs;
	}
}
