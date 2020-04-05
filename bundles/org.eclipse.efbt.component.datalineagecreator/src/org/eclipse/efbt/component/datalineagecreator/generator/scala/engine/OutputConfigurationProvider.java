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
 *  Neil Mackenzie - initial API and implementation
 *******************************************************************************/
package org.eclipse.efbt.component.datalineagecreator.generator.scala.engine;

import static org.eclipse.xtext.xbase.lib.CollectionLiterals.newHashSet;
import java.util.Set;
import org.eclipse.xtext.generator.IFileSystemAccess;
import org.eclipse.xtext.generator.IOutputConfigurationProvider;
import org.eclipse.xtext.generator.OutputConfiguration;

/**
 * @author Neil Mackenzie
 *
 */
public class OutputConfigurationProvider implements IOutputConfigurationProvider {

	/**
	 * GEN_ONCE_OUTPUT
	 */
	public static final String GEN_ONCE_OUTPUT = "gen-once";

	/**
	 * get Output Configurations
	 * 
	 * @return a set of {@link OutputConfiguration} available for the generator
	 */
	public Set<OutputConfiguration> getOutputConfigurations() {
		OutputConfiguration defaultOutput = new OutputConfiguration(IFileSystemAccess.DEFAULT_OUTPUT);
		defaultOutput.setDescription("Output folder");
		defaultOutput.setOutputDirectory("./src");
		//defaultOutput.setOutputDirectory("C:\\Users\\Neil\\git\\org.eclipse.runtime.basel.frtb.ec.scala\\org.eclipse.runtime.basel.frtb.ec.scala\\src\\");
		defaultOutput.setOverrideExistingResources(true);
		defaultOutput.setCreateOutputDirectory(false);
		defaultOutput.setCleanUpDerivedResources(false);
		defaultOutput.setSetDerivedProperty(true);

		OutputConfiguration readonlyOutput = new OutputConfiguration(GEN_ONCE_OUTPUT);
		readonlyOutput.setDescription("Generate once output folder");
		readonlyOutput.setOutputDirectory("./src");
		readonlyOutput.setOverrideExistingResources(false);
		readonlyOutput.setCreateOutputDirectory(false);
		readonlyOutput.setCleanUpDerivedResources(false);
		readonlyOutput.setSetDerivedProperty(false);
		return newHashSet(defaultOutput, readonlyOutput);
	}
	
}
