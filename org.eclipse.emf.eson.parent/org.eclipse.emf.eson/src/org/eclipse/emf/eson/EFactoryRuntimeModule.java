/*
 * #%L
 * org.eclipse.emf.eson
 * %%
 * Copyright (C) 2009 - 2014 Sebastian Benz & Michael Vorburger
 * %%
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 * #L%
 */
package org.eclipse.emf.eson;

import org.apache.log4j.Logger;
import org.eclipse.emf.eson.conversion.DATEValueConverter;
import org.eclipse.emf.eson.conversion.TerminalConverters;
import org.eclipse.emf.eson.generators.DelegatingGenerator;
import org.eclipse.emf.eson.generators.EFactoryFileExtensionRegistry;
import org.eclipse.emf.eson.resource.EFactoryDerivedStateComputer;
import org.eclipse.emf.eson.resource.EFactoryEncodingProvider;
import org.eclipse.emf.eson.resource.EFactoryLocationInFileProvider;
import org.eclipse.emf.eson.resource.EFactoryResourceFactory;
import org.eclipse.emf.eson.resource.EFactoryResourceServiceProvider;
import org.eclipse.emf.eson.scoping.EFactoryQualifiedNameProvider;
import org.eclipse.emf.eson.scoping.ESONLinkingService;
import org.eclipse.emf.eson.scoping.ExtendedImportedNamespaceAwareLocalScopeProvider;
import org.eclipse.emf.eson.scoping.WarningErrorHandlerWithoutNoSuchMethodException;
import org.eclipse.emf.eson.validation.ESONDiagnosticConverter;
import org.eclipse.emf.eson.validation.ESONLinkingDiagnosticMessageProvider;
import org.eclipse.emf.eson.xtextbackpatch.FasterResourceSetBasedAllContainersStateProvider;
import org.eclipse.xtext.Constants;
import org.eclipse.xtext.generator.IGenerator;
import org.eclipse.xtext.linking.ILinkingDiagnosticMessageProvider;
import org.eclipse.xtext.linking.ILinkingService;
import org.eclipse.xtext.naming.IQualifiedNameProvider;
import org.eclipse.xtext.parser.IEncodingProvider;
import org.eclipse.xtext.parser.antlr.IReferableElementsUnloader;
import org.eclipse.xtext.parser.antlr.IReferableElementsUnloader.GenericUnloader;
import org.eclipse.xtext.resource.IDerivedStateComputer;
import org.eclipse.xtext.resource.ILocationInFileProvider;
import org.eclipse.xtext.resource.IResourceFactory;
import org.eclipse.xtext.resource.IResourceServiceProvider;
import org.eclipse.xtext.scoping.IScope;
import org.eclipse.xtext.scoping.IScopeProvider;
import org.eclipse.xtext.scoping.impl.AbstractDeclarativeScopeProvider;
import org.eclipse.xtext.service.DispatchingProvider;
import org.eclipse.xtext.util.PolymorphicDispatcher;
import org.eclipse.xtext.validation.IDiagnosticConverter;
import org.eclipse.xtext.validation.IResourceValidator;
import org.eclipse.xtext.xbase.lib.IterableExtensions;

import com.google.inject.Binder;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Names;

/**
 * Use this class to register components to be used within the IDE.
 */
public class EFactoryRuntimeModule extends AbstractEFactoryRuntimeModule {

	@Override
	public void configure(Binder binder) {
		super.configure(binder);
		binder.bind(String.class)
				.annotatedWith(Names.named(DATEValueConverter.DATE_FORMAT_KEY))
				.toInstance("MM.dd.yyyy");
		binder.bind(IReferableElementsUnloader.class).to(GenericUnloader.class);
	}

	@Override
	public Class<? extends org.eclipse.xtext.conversion.IValueConverterService> bindIValueConverterService() {
		return TerminalConverters.class;
	}

	@Override
	public Class<? extends IResourceFactory> bindIResourceFactory() {
		return EFactoryResourceFactory.class;
	}

	public Class<? extends IDiagnosticConverter> bindIDiagnosticConverter() {
		return ESONDiagnosticConverter.class;
	}

	// NO @Override
	public Class<? extends IDerivedStateComputer> bindIDerivedStateComputer() {
		return EFactoryDerivedStateComputer.class;
	}

	// http://www.eclipse.org/forums/index.php/m/759778/
	public void configureErrorHandler(Binder binder) {
		Logger logger = Logger.getLogger(PolymorphicDispatcher.class);
		PolymorphicDispatcher.ErrorHandler<IScope> handler = new WarningErrorHandlerWithoutNoSuchMethodException<IScope>(
				logger);

		binder.bind(
				new TypeLiteral<PolymorphicDispatcher.ErrorHandler<IScope>>() {
				})
				.annotatedWith(
						Names.named(AbstractDeclarativeScopeProvider.NAMED_ERROR_HANDLER))
				.toInstance(handler);
	}

	@Override
	public Class<? extends ILocationInFileProvider> bindILocationInFileProvider() {
		return EFactoryLocationInFileProvider.class;
	}

	public Class<? extends org.eclipse.xtext.resource.IResourceDescription.Manager> bindIResourceDescription$Manager() {
		return org.eclipse.xtext.resource.DerivedStateAwareResourceDescriptionManager.class;
	}

	@SuppressWarnings("restriction")
	public Class<? extends IResourceValidator> bindIResourceValidator() {
		return org.eclipse.xtext.xbase.annotations.validation.DerivedStateAwareResourceValidator.class;
	}

	public Class<? extends IGenerator> bindIGenerator() {
		return DelegatingGenerator.class;
	}

	public Class<? extends IResourceServiceProvider> bindIResourceServiceProvider() {
		return EFactoryResourceServiceProvider.class;
	}

	@Override
	public Class<? extends IQualifiedNameProvider> bindIQualifiedNameProvider() {
		return EFactoryQualifiedNameProvider.class;
	}

	public Class<? extends ILinkingDiagnosticMessageProvider> bindILinkingDiagnosticMessageProvider() {
		return ESONLinkingDiagnosticMessageProvider.class;
	}

	@Override
	public void configureIScopeProviderDelegate(com.google.inject.Binder binder) {
		binder.bind(IScopeProvider.class)
				.annotatedWith(
						Names.named(AbstractDeclarativeScopeProvider.NAMED_DELEGATE))
				.to(ExtendedImportedNamespaceAwareLocalScopeProvider.class);
	}

	@Override
	public Class<? extends ILinkingService> bindILinkingService() {
		return ESONLinkingService.class;
	}

	@Override
	public void configureRuntimeEncodingProvider(Binder binder) {
		binder.bind(IEncodingProvider.class)
				.annotatedWith(DispatchingProvider.Runtime.class)
				.to(EFactoryEncodingProvider.class);
	}

	@Override
	// TODO Remove once ESON is migrated to support only Xtext 2.9.0 and no
	// 2.8.x anymore
	public Class<? extends org.eclipse.xtext.resource.containers.IAllContainersState.Provider> bindIAllContainersState$Provider() {
		return FasterResourceSetBasedAllContainersStateProvider.class;
	}

	@Override
	public void configureFileExtensions(Binder binder) {
		if (properties == null
				|| properties.getProperty(Constants.FILE_EXTENSIONS) == null) {
			binder.bind(String.class)
					.annotatedWith(Names.named(Constants.FILE_EXTENSIONS))
					.toInstance(
							IterableExtensions
									.join(EFactoryFileExtensionRegistry
											.getRegistry()
											.getAssociateFileExtensionsWithDefaults(),
											","));
		}
	}
}
