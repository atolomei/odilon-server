package io.odilon.vfs.raid6;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import io.odilon.model.ODModelObject;


public class EncodedInfo extends ODModelObject {

	public List<File> encodedBlocks = new ArrayList<File>();
	public long fileSize;
	
	
	public EncodedInfo() {
		
	}
	
	
}
