/*******************************************************************************
 * Copyright (c) 2020 Bird Software Solutions Ltd
 * This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License 2.0
 * which accompanies this distribution, and is available at
 * https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
 *
 * Contributors:
 *    Neil Mackenzie - initial API and implementation
 *******************************************************************************/
package ecore4reg.model.sdd_model;

public class ImportOfBIRDFromMSAccess {

	
	
	
	public static void importBIRDFromMSAccess(String filepath, String outputFilepath)
	{
		Importer  importer = new BIRDImporterImpl(); 
		importer.doImport(filepath,outputFilepath);
		importer.saveArtifactsAsJSON();
	}
	
	

	
	
}
