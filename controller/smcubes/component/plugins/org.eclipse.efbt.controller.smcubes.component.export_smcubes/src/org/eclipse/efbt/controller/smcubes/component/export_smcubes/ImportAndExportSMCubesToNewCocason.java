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
package org.eclipse.efbt.controller.smcubes.component.export_smcubes;


import org.eclipse.efbt.controller.smcubes.component.importexport.impl.BIRDImporterImpl;
import org.eclipse.efbt.controller.smcubes.component.importexport.impl.Importer;




public class ImportAndExportSMCubesToNewCocason {

	
	
	
	public static void translateBIRDWithNewTestFormatToCoCaLiMo(String filepath, String outputFilepath, String testdatafilepath)
	{
		Importer  importer = new BIRDImporterImpl(); 
		importer.doImport(filepath,outputFilepath,testdatafilepath);
		importer.importTestDataWithOldTestFormat(testdatafilepath);
		importer.saveArtifactsAsJSON();
	}
	
	

	
	
}
