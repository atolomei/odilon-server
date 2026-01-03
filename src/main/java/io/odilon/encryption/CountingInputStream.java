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

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
 
public final class CountingInputStream extends FilterInputStream {

 private final AtomicLong count = new AtomicLong(0);
 private final AtomicBoolean closed = new AtomicBoolean(false);

 public CountingInputStream(InputStream in) {
     super(in);
 }

 @Override
 public int read() throws IOException {
     int r = super.read();
     if (r != -1) count.incrementAndGet();
     return r;
 }

 @Override
 public int read(byte[] b, int off, int len) throws IOException {
     int n = super.read(b, off, len);
     if (n > 0) count.addAndGet(n);
     return n;
 }

 @Override
 public long skip(long n) throws IOException {
     long s = super.skip(n);
     if (s > 0) count.addAndGet(s);
     return s;
 }

 @Override
 public void close() throws IOException {
     if (closed.compareAndSet(false, true)) {
         super.close();
     }
 }

 public long getCount() {
     return count.get();
 }

 public boolean isClosed() {
     return closed.get();
 }
 

    
}
