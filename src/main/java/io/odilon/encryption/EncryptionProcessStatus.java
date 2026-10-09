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
package io.odilon.encryption;

import java.io.Serializable;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * <p>
 * Status snapshot of the async process that encrypts Objects stored
 * unencrypted ({@link ObjectEncryptionProcess}).
 * </p>
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
public class EncryptionProcessStatus implements Serializable {

	private static final long serialVersionUID = 1L;

	public static final String IDLE = "IDLE";
	public static final String RUNNING = "RUNNING";
	public static final String COMPLETED = "COMPLETED";
	public static final String FAILED = "FAILED";

	@JsonProperty("status")
	public String status = IDLE;

	/** bucket filter of the last / current run, null = all buckets */
	@JsonProperty("bucket")
	public String bucket;

	@JsonProperty("currentBucket")
	public String currentBucket;

	@JsonProperty("started")
	public OffsetDateTime started;

	@JsonProperty("finished")
	public OffsetDateTime finished;

	@JsonProperty("threads")
	public int threads = 0;

	/** objects listed */
	@JsonProperty("scanned")
	public long scanned = 0;

	/** objects re-stored encrypted */
	@JsonProperty("encrypted")
	public long encrypted = 0;

	/** objects already encrypted (or encrypted concurrently by a client) */
	@JsonProperty("skipped")
	public long skipped = 0;

	/** objects listed but not available (deleted / not ok) */
	@JsonProperty("notAvailable")
	public long notAvailable = 0;

	@JsonProperty("errors")
	public long errors = 0;

	/** plaintext bytes re-encrypted */
	@JsonProperty("totalBytes")
	public long totalBytes = 0;

	@JsonProperty("durationMillis")
	public long durationMillis = 0;

	@JsonProperty("message")
	public String message;

	public EncryptionProcessStatus() {
	}

	/** formatted multi-line block for the console / logs */
	public String console() {
		DateTimeFormatter fmt = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
		StringBuilder str = new StringBuilder();
		str.append("Object Encryption Process").append(System.lineSeparator());
		str.append(String.format("Status:         %s%n", status));
		str.append(String.format("Bucket:         %s%n", bucket != null ? bucket : "(all)"));
		str.append(String.format("Started:        %s%n", started != null ? fmt.format(started) : "-"));
		str.append(String.format("Finished:       %s%n", finished != null ? fmt.format(finished) : "-"));
		str.append(String.format("Threads:        %,d%n", threads));
		str.append(String.format("Scanned:        %,d%n", scanned));
		str.append(String.format("Encrypted:      %,d%n", encrypted));
		str.append(String.format("Skipped:        %,d%n", skipped));
		str.append(String.format("Not available:  %,d%n", notAvailable));
		str.append(String.format("Errors:         %,d%n", errors));
		str.append(String.format("Total bytes:    %,d%n", totalBytes));
		str.append(String.format("Duration:       %,d ms", durationMillis));
		if (message != null)
			str.append(System.lineSeparator()).append("Message:        ").append(message);
		return str.toString();
	}
}
