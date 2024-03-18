package io.odilon.vfs.raid6;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnore;

import io.odilon.model.ODModelObject;


/**
 * 
 * <p>List of encoded blocks stored in File System
 *  See the coding convention: {@link RAIDSixDriver}</p> 
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
public class RAIDSixBlocks extends ODModelObject {

	@JsonIgnore
	public List<File> encodedBlocks = new ArrayList<File>();
	
	@JsonIgnore
	public long fileSize;
	
	public RAIDSixBlocks() {
	}

	@Override
	public String toJSON() {
		StringBuilder str = new StringBuilder();
		if (encodedBlocks!=null) {
			str.append("[");
			encodedBlocks.forEach(f -> str.append("\"" + f.getName() + "\" "));
			str.append("]");
		}
		str.append("\"fileSize\":\"" + String.valueOf(fileSize) +"\"");
		return str.toString();		
	}

	
	
}
