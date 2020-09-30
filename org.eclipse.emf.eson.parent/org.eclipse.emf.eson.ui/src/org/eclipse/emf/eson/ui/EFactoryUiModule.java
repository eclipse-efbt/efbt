/*
 * #%L
 * org.eclipse.emf.eson.ui
 * %%
 * Copyright (C) 2009 - 2014 Sebastian Benz, Michael Vorburger and others
 * %%
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 * #L%
 */
package org.eclipse.emf.eson.ui;

import org.eclipse.emf.eson.resource.EFactoryEncodingProvider;
import org.eclipse.emf.eson.resource.EFactoryResourceFactory;
import org.eclipse.emf.eson.ui.contentassist.EFactoryContentAssistantFactory;
import org.eclipse.emf.eson.ui.contentassist.IEAttributeStringProposalProvider;
import org.eclipse.emf.eson.ui.contentassist.TerminalsEAttributeStringProposalProvider;
import org.eclipse.ui.plugin.AbstractUIPlugin;
import org.eclipse.ui.views.contentoutline.IContentOutlinePage;
import org.eclipse.xtext.parser.IEncodingProvider;
import org.eclipse.xtext.resource.IResourceFactory;
import org.eclipse.xtext.service.DispatchingProvider;
import org.eclipse.xtext.ui.editor.contentassist.IContentAssistantFactory;
import org.eclipse.xtext.ui.editor.contentassist.XtextContentAssistProcessor;

import com.google.inject.Binder;
import com.google.inject.name.Names;

/**
 * Use this class to register components to be used within the IDE.
 */
public class EFactoryUiModule extends AbstractEFactoryUiModule {

	public EFactoryUiModule(AbstractUIPlugin plugin) {
		super(plugin);
	}

	@Override
	public void configure(Binder binder) {
		super.configure(binder);
		
		// @see http://christiandietrich.wordpress.com/2011/09/19/xtext-content-assist-auto-activation/
		binder.bind(String.class)
				.annotatedWith(
						Names.named(XtextContentAssistProcessor.COMPLETION_AUTO_ACTIVATION_CHARS))
				.toInstance(":{"); //$NON-NLS-1$

		binder.bind(IResourceFactory.class).to(EFactoryResourceFactory.class);
	}
	
	
	@Override
	public Class<? extends IContentAssistantFactory> bindIContentAssistantFactory() {
		return EFactoryContentAssistantFactory.class;
	}

	
	
	public Class<? extends IEAttributeStringProposalProvider> bindIEAttributeProposalProvider() {
		return TerminalsEAttributeStringProposalProvider.class;
	}

	
	


    @Override
    public Class<? extends IContentOutlinePage> bindIContentOutlinePage() {
        return null;
    }

   
    
    @Override
    public void configureUiEncodingProvider(Binder binder) {
        binder.bind(IEncodingProvider.class).annotatedWith(DispatchingProvider.Ui.class).to(EFactoryEncodingProvider.class);
    }

}
