/*
 * Odilon Object Storage
 * (C) Novamens 
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
package io.odilon;

public class OdilonVersion {

	public static final String VERSION = "1.8";
	
	private static String[] brand_char = null;
	
	public static String[] getAppCharacterName() {
    
		if (brand_char != null)
            return brand_char;
		
        brand_char = new String[9];
        brand_char[0] = odilon[0] + "";
        brand_char[1] = odilon[1] + "";
        brand_char[2] = odilon[2] + "";
        brand_char[3] = odilon[3] + ""; 
        brand_char[4] = odilon[4] + "";
        brand_char[5] = odilon[5];
        brand_char[6] = "";
        brand_char[7] = "version: " + VERSION;
        brand_char[8] = "";
        return brand_char;
    }
    
    static final String odilon[] = {
	" ________   _______     _____   __         ________   ___    __  ", 
	"/  ____  \\  |  ___  \\  |_   _|  | |       /  ____  \\  |  \\   | |",
	"| |    | |  | |   \\  |   | |    | |       | |    | |  |   \\  | |",
	"| |    | |  | |    | |   | |    | |       | |    | |  | |\\ \\ | |",
	"| |____| |  | |___/  |  _| |_   | |____   | |____| |  | | \\ \\| |",
	"\\________/  |_______/  |_____|  |______|  \\________/  |_|  \\___|" };

    
}
