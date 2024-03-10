package io.odilon.vfs.raid6;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import io.odilon.model.ODModelObject;


/**
 * 
 * 
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
public class RSFileBlocks extends ODModelObject {

	
	/**
	 * <p>See the coding convention: {@link RAIDSixDriver} 
	 *  </p>
	 */
	public List<File> encodedBlocks = new ArrayList<File>();
	
	
	/**
	 * 
	 * 
	 */
	public long fileSize;
	
	
	public RSFileBlocks() {
		
	}
	
	
}
