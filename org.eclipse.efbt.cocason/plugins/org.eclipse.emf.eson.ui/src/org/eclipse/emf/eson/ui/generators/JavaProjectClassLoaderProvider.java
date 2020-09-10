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
package org.eclipse.emf.eson.ui.generators;

import org.eclipse.emf.ecore.resource.Resource;
import org.eclipse.emf.ecore.resource.ResourceSet;
import org.eclipse.emf.eson.generators.ClassLoaderProvider;
import org.eclipse.emf.eson.generators.DelegatingClassLoader;
import org.eclipse.jdt.core.IJavaProject;
import org.eclipse.xtext.generator.IGenerator;
import org.eclipse.xtext.resource.XtextResourceSet;

import com.google.inject.Inject;

/**
 * ClassLoaderProvider using Xtend (currently) / JDT to find correct classpath of Java project.
 *  
 * @author Michael Vorburger
 */
public class JavaProjectClassLoaderProvider implements ClassLoaderProvider {

    protected @Inject ExtendedJdtBasedProcessorProvider extendedJdtBasedProcessorProvider; 
    
    @Override
    public ClassLoader getClassLoader(Resource resource) throws IllegalArgumentException {
        ResourceSet resourceSet = resource.getResourceSet();
        // originally inspired by / stolen from org.eclipse.xtext.common.types.access.ClasspathTypeProviderFactory.getClassLoader(ResourceSet)
        if (resourceSet instanceof XtextResourceSet) {
            XtextResourceSet xtextResourceSet = (XtextResourceSet) resourceSet;
            Object ctx = xtextResourceSet.getClasspathURIContext();
            if (ctx != null) {
                // NEW, added by Michael Vorburger (TODO Isn't there an existing helper in Xtext that does this?)
                if (ctx instanceof IJavaProject) {
                    IJavaProject javaProject = (IJavaProject) ctx;
                    Class<?> modelClass = resource.getContents().get(1).getClass(); // TODO harden this.. check if it's an ESON Resource, obtain first non-Factory instance 
                    ClassLoader delegatingClassLoader = new DelegatingClassLoader(IGenerator.class.getClassLoader(), modelClass.getClassLoader());
                    extendedJdtBasedProcessorProvider.setParentClassLoader(delegatingClassLoader);
                    return extendedJdtBasedProcessorProvider.createClassLoaderForJavaProject(javaProject); // TODO Perhaps this should be cached?
                }
                if (ctx instanceof Class<?>) {
                    return ((Class<?>)ctx).getClassLoader();
                }
                if (!(ctx instanceof ClassLoader)) {
                    return ctx.getClass().getClassLoader();
                }
                return (ClassLoader) ctx;
            }
        }
        throw new IllegalArgumentException("Cannot obtain a ClassLoader for this ResourceSet: " + resourceSet.toString());
    }

}
