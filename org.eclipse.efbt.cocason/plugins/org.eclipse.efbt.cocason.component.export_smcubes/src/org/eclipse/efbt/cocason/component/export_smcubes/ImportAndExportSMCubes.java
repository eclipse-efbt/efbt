/*
 * #%L
 * org.eclipse.emf.eson
 * %%
 * Copyright (C) 2020 Bird Software Solutions
 * %%
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 * #L%
 */
package org.eclipse.efbt.cocason.component.export_smcubes;

import org.eclipse.efbt.cocamo.smcubes.component.importexport.impl.BIRDImporterImpl;
import org.eclipse.efbt.cocamo.smcubes.component.importexport.impl.Importer;
import org.eclipse.efbt.cocamo.smcubes.model.smcubes_model.SmcubesModel;

public class ImportAndExportSMCubes {

	
	public void importAndExportSMCubes(String filepath, String outputFilepath, String testdatafilepath)
	{
		Importer  importer = new BIRDImporterImpl(); 
		importer.doImport(filepath,outputFilepath,testdatafilepath);
		SmcubesModel birdModel = importer.getBirdModel();
		CoCaSonSMCubesPersister persister = new CoCaSonSMCubesPersister();
		persister.saveArtifactsAsCoCaSon(birdModel, outputFilepath);
	}
}
