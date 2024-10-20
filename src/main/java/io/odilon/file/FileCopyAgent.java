package io.odilon.file;

import io.odilon.model.BaseObject;

public abstract class FileCopyAgent extends BaseObject {
	
	public FileCopyAgent() {
	}
	
	public abstract long durationMillisecs();
	public abstract boolean execute();
	
}

