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
	 * 
	 *  objectName.[chunk#].[block#]
	 * objectName.[chunk#].[block#].v[version#]
 
		where: 
		chunk# 		0..total_chunks, depending of the size of the file to encode, ServerConstant.MAX_CHUNK_SIZE =  32 MB,
					this means that for files smaller or equal to 32 MB there will be only one chunk (chunk=0), for
					files up to 64 MB there will be 2 chunks and so on.
					
		block# 		is the disk [0..(data+parity-1)]
		
		version# 	is omitted for head version.

	 * 
	 *
	 * 
	 * 
	 * 
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
