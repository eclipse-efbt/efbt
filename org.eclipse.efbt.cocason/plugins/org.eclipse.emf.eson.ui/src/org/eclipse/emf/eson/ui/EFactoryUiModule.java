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

import org.eclipse.emf.eson.generators.ClassLoaderProvider;
import org.eclipse.emf.eson.resource.EFactoryEncodingProvider;
import org.eclipse.emf.eson.resource.EFactoryResourceFactory;
import org.eclipse.emf.eson.ui.contentassist.EFactoryContentAssistantFactory;
import org.eclipse.emf.eson.ui.contentassist.IEAttributeStringProposalProvider;
import org.eclipse.emf.eson.ui.contentassist.TerminalsEAttributeStringProposalProvider;
import org.eclipse.emf.eson.ui.editor.EFactoryFoldingRegionProvider;
import org.eclipse.emf.eson.ui.editor.model.edit.EFactoryTextEditComposer;
import org.eclipse.emf.eson.ui.generators.JavaProjectClassLoaderProvider;
import org.eclipse.emf.eson.ui.highlighting.EFactoryHighlightingCalculator;
import org.eclipse.emf.eson.ui.highlighting.EFactorySemanticHighlightingConfiguration;
import org.eclipse.emf.eson.ui.highlighting.EFactoryTokenTypeToStringMapper;
import org.eclipse.emf.eson.ui.outline.DisabledComparator;
import org.eclipse.ui.plugin.AbstractUIPlugin;
import org.eclipse.ui.views.contentoutline.IContentOutlinePage;
import org.eclipse.xtext.parser.IEncodingProvider;
import org.eclipse.xtext.resource.IResourceFactory;
import org.eclipse.xtext.service.DispatchingProvider;
import org.eclipse.xtext.ui.editor.contentassist.IContentAssistantFactory;
import org.eclipse.xtext.ui.editor.contentassist.XtextContentAssistProcessor;
import org.eclipse.xtext.ui.editor.folding.IFoldingRegionProvider;
import org.eclipse.xtext.ui.editor.model.edit.ITextEditComposer;
import org.eclipse.xtext.ui.editor.outline.impl.OutlineFilterAndSorter.IComparator;
import org.eclipse.xtext.ui.editor.syntaxcoloring.AbstractAntlrTokenToAttributeIdMapper;
import org.eclipse.xtext.ui.editor.syntaxcoloring.ISemanticHighlightingCalculator;

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
	
	public Class<? extends ITextEditComposer> bindITextEditComposer() {
		return EFactoryTextEditComposer.class;
	}

	@Override
	public Class<? extends IContentAssistantFactory> bindIContentAssistantFactory() {
		return EFactoryContentAssistantFactory.class;
	}

	public Class<? extends IFoldingRegionProvider> bindIFoldingRegionProvider() {
		return EFactoryFoldingRegionProvider.class;
	}
	
	public Class<? extends IEAttributeStringProposalProvider> bindIEAttributeProposalProvider() {
		return TerminalsEAttributeStringProposalProvider.class;
	}

	public Class<? extends org.eclipse.xtext.ui.editor.syntaxcoloring.IHighlightingConfiguration> bindIHighlightingConfiguration() {
		return EFactorySemanticHighlightingConfiguration.class;
	}
	
	public Class<? extends ISemanticHighlightingCalculator> bindISemanticHighlightingCalculator() {
		return EFactoryHighlightingCalculator.class;
	}
	
	public Class<? extends AbstractAntlrTokenToAttributeIdMapper> bindAbstractAntlrTokenToAttributeIdMapper() {
		return EFactoryTokenTypeToStringMapper.class;
	}

    public Class<? extends ClassLoaderProvider> bindJavaProjectClassLoaderProvider() {
        return JavaProjectClassLoaderProvider.class;
    }

    @Override
    public Class<? extends IContentOutlinePage> bindIContentOutlinePage() {
        return null;
    }

    @Override
    public Class<? extends IComparator> bindOutlineFilterAndSorter$IComparator() {
        return DisabledComparator.class;
    }
    
    @Override
    public void configureUiEncodingProvider(Binder binder) {
        binder.bind(IEncodingProvider.class).annotatedWith(DispatchingProvider.Ui.class).to(EFactoryEncodingProvider.class);
    }

}
