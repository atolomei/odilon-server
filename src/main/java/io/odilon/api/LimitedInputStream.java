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
package io.odilon.api;

import java.io.IOException;
import java.io.InputStream;

public class LimitedInputStream extends InputStream {

	private final InputStream in;
	private long remaining;

	public LimitedInputStream(InputStream in, long limit) {
		this.in = in;
		this.remaining = limit;
	}

	@Override
	public int read() throws IOException {
		if (remaining <= 0)
			return -1;
		int b = in.read();
		if (b != -1)
			remaining--;
		return b;
	}

	@Override
	public int read(byte[] b, int off, int len) throws IOException {
		if (remaining <= 0)
			return -1;
		int toRead = (int) Math.min(len, remaining);
		int r = in.read(b, off, toRead);
		if (r != -1)
			remaining -= r;
		return r;
	}

	@Override
	public void close() throws IOException {
		in.close();
	}

}
