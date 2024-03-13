package io.odilon.vfs.raid6;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnore;

import io.odilon.model.ODModelObject;


/**
 * 
 * 
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
public class RAIDSixBlocks extends ODModelObject {

	
	/**
	 * <p>See the coding convention: {@link RAIDSixDriver} 
	 *  </p>
	 */
	
	@JsonIgnore
	public List<File> encodedBlocks = new ArrayList<File>();
	
	
	@JsonIgnore
	public long fileSize;
	
	
	public RAIDSixBlocks() {
		
	}
	
	
}
