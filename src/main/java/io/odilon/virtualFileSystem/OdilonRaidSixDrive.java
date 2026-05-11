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
package io.odilon.virtualFileSystem;

import org.springframework.stereotype.Component;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

import io.odilon.log.Logger;
import io.odilon.model.RedundancyLevel;
import io.odilon.util.Check;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;

/**
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
@JsonInclude(Include.NON_NULL)
@Component
@Scope("prototype")
public class OdilonRaidSixDrive extends OdilonDrive {

	@SuppressWarnings("unused")
	static private Logger startuplogger = Logger.getLogger("StartupLogger");
	@SuppressWarnings("unused")
	static private Logger logger = Logger.getLogger(OdilonRaidSixDrive.class.getName());

	
	@Autowired
	protected OdilonRaidSixDrive(String rootDir) {
				super(rootDir);
	}

	/**
	 * <p>
	 * Constructor to call when creating a Dir with {@code new Drive}. <br/>
	 * it calls method {@link onInitialize()}
	 * </p>
	 * 
	 * @param name
	 * @param rootDir
	 */
	protected OdilonRaidSixDrive(String name, String rootDir, int configOrder, String raidSetup, int raidDrives) {
		super(name, rootDir, configOrder, raidSetup, raidDrives);
		Check.requireTrue(raidSetup.equals(RedundancyLevel.RAID_6.getName()),"raidSetup must be " + RedundancyLevel.RAID_6.getName() + " and it is -> " + raidSetup);
	}

}
