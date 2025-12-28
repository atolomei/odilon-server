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
	        if (remaining <= 0) return -1;
	        int b = in.read();
	        if (b != -1) remaining--;
	        return b;
	    }

	    @Override
	    public int read(byte[] b, int off, int len) throws IOException {
	        if (remaining <= 0) return -1;
	        int toRead = (int) Math.min(len, remaining);
	        int r = in.read(b, off, toRead);
	        if (r != -1) remaining -= r;
	        return r;
	    }

	    @Override
	    public void close() throws IOException {
	        in.close();
	    }

}
