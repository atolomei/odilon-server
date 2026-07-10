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

import java.io.InputStream;

// EncryptedResult.java (inner class or top-level)

public class EncryptedResult {

	private final InputStream inputStream;

	// If totalLengthKnown is true, totalLength contains the final value.
	// If false, countingStream will provide live/count-after-close value.

	private final boolean totalLengthKnown;
	private final long totalLength;
	private final CountingInputStream countingStream; // may be null if known

	public EncryptedResult(InputStream inputStream, long totalLength) {
		this.inputStream = inputStream;
		this.totalLengthKnown = true;
		this.totalLength = totalLength;
		this.countingStream = null;
	}

	public EncryptedResult(InputStream inputStream, CountingInputStream countingStream) {
		this.inputStream = inputStream;
		this.totalLengthKnown = false;
		this.totalLength = -1;
		this.countingStream = countingStream;
	}

	public InputStream getInputStream() {
		return inputStream;
	}

	/**
	 * True if total encrypted length is known up front (no streaming counting
	 * needed).
	 */
	public boolean isTotalLengthKnown() {
		return totalLengthKnown;
	}

	/**
	 * Returns total length if known up front, otherwise throws
	 * IllegalStateException.
	 */
	public long getTotalLengthIfKnown() {
		if (!totalLengthKnown)
			throw new IllegalStateException("Total length not known up front");
		return totalLength;
	}

	/**
	 * Returns the counting value. If the stream is still being read, this returns
	 * the bytes read so far. If the stream was closed, this returns the final
	 * encrypted payload bytes count (not including JSON header).
	 *
	 * To compute final total encrypted length (including JSON header), do:
	 * jsonBytes.length + countingStream.getCount() -- but you must know
	 * jsonBytes.length in your code
	 */
	public CountingInputStream getCountingStream() {
		if (countingStream == null)
			throw new IllegalStateException("Counting stream not available");
		return countingStream;
	}
}
