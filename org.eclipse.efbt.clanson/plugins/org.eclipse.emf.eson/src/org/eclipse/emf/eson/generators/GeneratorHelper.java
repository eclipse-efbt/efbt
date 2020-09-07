/*
 * #%L
 * org.eclipse.emf.eson
 * %%
 * Copyright (C) 2015 Michael Vorburger
 * %%
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 * #L%
 */
package org.eclipse.emf.eson.generators;

import org.eclipse.emf.common.util.EList;
import org.eclipse.emf.common.util.URI;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.resource.Resource;
import org.eclipse.xtext.generator.IFileSystemAccess;

import com.google.common.base.Optional;

/**
 * Utility with helper methods useful in IGenerator implementations.
 * 
 * @author Michael Vorburger
 */
public class GeneratorHelper {

	/**
	 * Find the first occurrence of content of a certain type.
	 * 
	 * This helper intentionally, for optional performance, does NOT traverse the entire Resource, but only the first level.
	 * 
	 * @param resource EMF Resource
	 * @param klass Java class to find in first level of getContents() of Resource
	 * @return Guava optional
	 */
	public <T extends EObject> Optional<T> getFirstRootContentOfType(Resource resource, Class<T> klass) {
		if (!resource.isLoaded())
			return Optional.absent();

		EList<EObject> contents = resource.getContents();
		for (EObject eObject : contents) {
			if (klass.isInstance(eObject)) {
				return Optional.of(klass.cast(eObject));
			}
		}
		return Optional.absent();
	}

	// TODO ideally this should return a relative path which does NOT include the "source folder" of the resourceURI.. how-to? 
	public String getProjectRelativeFileName(URI resourceURI, IFileSystemAccess fsa) {
		// @see http://www.eclipse.org/forums/index.php/m/1230878/#msg_1230878
		if (!resourceURI.isPlatformResource())
			throw new IllegalArgumentException("Not a Platform Resource URI: " + resourceURI.toString());
		// This is  bit of a hack, but it works...
		String sURI = resourceURI.toPlatformString(true);
		String withoutProject = sURI.substring(sURI.indexOf('/', 1) + 1);
		return withoutProject;
		// Something like this may be a better use of the API, but is much more difficult to unit test in EFactoryJSONGeneratorTest, so not pursued: 
		// URI projectRootURI = ((IFileSystemAccessExtension2)fsa).getURI(".");
		// URI resourceWithoutProjectURI = resourceURI.deresolve(projectRootURI);
		// return resourceWithoutProjectURI.toString();
	}

	protected String removeExtension(String path) {
		// LIKE FilenameUtils.removeExtension(relFileName)
		int extensionPos = path.lastIndexOf('.');
		return path.substring(0, extensionPos);		
	}

	public String replaceExtension(String path, String newExtension) {
		return new StringBuilder(removeExtension(path)).append('.').append(newExtension).toString();
	}

}