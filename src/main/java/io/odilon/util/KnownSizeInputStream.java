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
package io.odilon.util;

import java.io.FilterInputStream;
import java.io.InputStream;

/**
 * <p>
 * {@link InputStream} wrapper that carries the total size (in bytes) of the
 * data, when it is known in advance by the caller (e.g. multipart upload,
 * local file).
 * </p>
 * <p>
 * It does not alter the data in any way. The RAID handlers use the size hint
 * to decide, <b>before</b> writing the first byte, whether the Object can be
 * encrypted (see {@code encryption.maxObjectSize} in
 * {@code odilon.properties}). If the stream is not a
 * {@code KnownSizeInputStream} the size is considered unknown and the Object
 * is encrypted as usual (if it turns out to be too large the operation fails
 * and is rolled back).
 * </p>
 *
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
public class KnownSizeInputStream extends FilterInputStream {

	private final long knownSize;

	/**
	 * @param in        stream to wrap, can not be null
	 * @param knownSize total size in bytes of the data in {@code in}, must be
	 *                  &gt;= 0
	 */
	public KnownSizeInputStream(InputStream in, long knownSize) {
		super(in);
		Check.requireNonNullArgument(in, "in is null");
		Check.requireTrue(knownSize >= 0, "knownSize must be >= 0");
		this.knownSize = knownSize;
	}

	/** @return total size in bytes of the data */
	public long getKnownSize() {
		return this.knownSize;
	}
}
