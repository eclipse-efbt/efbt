/**
 */
package org.eclipse.efbt.cocalimo.core.model.platform_call.provider;

import java.util.ArrayList;
import java.util.Collection;

import org.eclipse.efbt.cocalimo.core.model.platform_call.util.Platform_callAdapterFactory;

import org.eclipse.emf.common.notify.Adapter;
import org.eclipse.emf.common.notify.Notification;
import org.eclipse.emf.common.notify.Notifier;

import org.eclipse.emf.edit.provider.ChangeNotifier;
import org.eclipse.emf.edit.provider.ComposeableAdapterFactory;
import org.eclipse.emf.edit.provider.ComposedAdapterFactory;
import org.eclipse.emf.edit.provider.IChangeNotifier;
import org.eclipse.emf.edit.provider.IDisposable;
import org.eclipse.emf.edit.provider.IEditingDomainItemProvider;
import org.eclipse.emf.edit.provider.IItemLabelProvider;
import org.eclipse.emf.edit.provider.IItemPropertySource;
import org.eclipse.emf.edit.provider.INotifyChangedListener;
import org.eclipse.emf.edit.provider.IStructuredItemContentProvider;
import org.eclipse.emf.edit.provider.ITreeItemContentProvider;

/**
 * This is the factory that is used to provide the interfaces needed to support Viewers.
 * The adapters generated by this factory convert EMF adapter notifications into calls to {@link #fireNotifyChanged fireNotifyChanged}.
 * The adapters also support Eclipse property sheets.
 * Note that most of the adapters are shared among multiple instances.
 * <!-- begin-user-doc -->
 * <!-- end-user-doc -->
 * @generated
 */
public class Platform_callItemProviderAdapterFactory extends Platform_callAdapterFactory implements ComposeableAdapterFactory, IChangeNotifier, IDisposable {
	/**
	 * This keeps track of the root adapter factory that delegates to this adapter factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected ComposedAdapterFactory parentAdapterFactory;

	/**
	 * This is used to implement {@link org.eclipse.emf.edit.provider.IChangeNotifier}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected IChangeNotifier changeNotifier = new ChangeNotifier();

	/**
	 * This keeps track of all the supported types checked by {@link #isFactoryForType isFactoryForType}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected Collection<Object> supportedTypes = new ArrayList<Object>();

	/**
	 * This constructs an instance.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Platform_callItemProviderAdapterFactory() {
		supportedTypes.add(IEditingDomainItemProvider.class);
		supportedTypes.add(IStructuredItemContentProvider.class);
		supportedTypes.add(ITreeItemContentProvider.class);
		supportedTypes.add(IItemLabelProvider.class);
		supportedTypes.add(IItemPropertySource.class);
	}

	/**
	 * This keeps track of the one adapter used for all {@link org.eclipse.efbt.cocalimo.core.model.platform_call.PlatformCall} instances.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected PlatformCallItemProvider platformCallItemProvider;

	/**
	 * This creates an adapter for a {@link org.eclipse.efbt.cocalimo.core.model.platform_call.PlatformCall}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Adapter createPlatformCallAdapter() {
		if (platformCallItemProvider == null) {
			platformCallItemProvider = new PlatformCallItemProvider(this);
		}

		return platformCallItemProvider;
	}

	/**
	 * This keeps track of the one adapter used for all {@link org.eclipse.efbt.cocalimo.core.model.platform_call.PlatformCallModule} instances.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected PlatformCallModuleItemProvider platformCallModuleItemProvider;

	/**
	 * This creates an adapter for a {@link org.eclipse.efbt.cocalimo.core.model.platform_call.PlatformCallModule}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Adapter createPlatformCallModuleAdapter() {
		if (platformCallModuleItemProvider == null) {
			platformCallModuleItemProvider = new PlatformCallModuleItemProvider(this);
		}

		return platformCallModuleItemProvider;
	}

	/**
	 * This keeps track of the one adapter used for all {@link org.eclipse.efbt.cocalimo.core.model.platform_call.ConvertSQLDeveloperModelToEcore} instances.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected ConvertSQLDeveloperModelToEcoreItemProvider convertSQLDeveloperModelToEcoreItemProvider;

	/**
	 * This creates an adapter for a {@link org.eclipse.efbt.cocalimo.core.model.platform_call.ConvertSQLDeveloperModelToEcore}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Adapter createConvertSQLDeveloperModelToEcoreAdapter() {
		if (convertSQLDeveloperModelToEcoreItemProvider == null) {
			convertSQLDeveloperModelToEcoreItemProvider = new ConvertSQLDeveloperModelToEcoreItemProvider(this);
		}

		return convertSQLDeveloperModelToEcoreItemProvider;
	}

	/**
	 * This keeps track of the one adapter used for all {@link org.eclipse.efbt.cocalimo.core.model.platform_call.ImportBIRDFromMSAccess} instances.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected ImportBIRDFromMSAccessItemProvider importBIRDFromMSAccessItemProvider;

	/**
	 * This creates an adapter for a {@link org.eclipse.efbt.cocalimo.core.model.platform_call.ImportBIRDFromMSAccess}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Adapter createImportBIRDFromMSAccessAdapter() {
		if (importBIRDFromMSAccessItemProvider == null) {
			importBIRDFromMSAccessItemProvider = new ImportBIRDFromMSAccessItemProvider(this);
		}

		return importBIRDFromMSAccessItemProvider;
	}

	/**
	 * This keeps track of the one adapter used for all {@link org.eclipse.efbt.cocalimo.core.model.platform_call.CreateLogicalTransformationViewForScope} instances.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected CreateLogicalTransformationViewForScopeItemProvider createLogicalTransformationViewForScopeItemProvider;

	/**
	 * This creates an adapter for a {@link org.eclipse.efbt.cocalimo.core.model.platform_call.CreateLogicalTransformationViewForScope}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Adapter createCreateLogicalTransformationViewForScopeAdapter() {
		if (createLogicalTransformationViewForScopeItemProvider == null) {
			createLogicalTransformationViewForScopeItemProvider = new CreateLogicalTransformationViewForScopeItemProvider(this);
		}

		return createLogicalTransformationViewForScopeItemProvider;
	}

	/**
	 * This returns the root adapter factory that contains this factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public ComposeableAdapterFactory getRootAdapterFactory() {
		return parentAdapterFactory == null ? this : parentAdapterFactory.getRootAdapterFactory();
	}

	/**
	 * This sets the composed adapter factory that contains this factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setParentAdapterFactory(ComposedAdapterFactory parentAdapterFactory) {
		this.parentAdapterFactory = parentAdapterFactory;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public boolean isFactoryForType(Object type) {
		return supportedTypes.contains(type) || super.isFactoryForType(type);
	}

	/**
	 * This implementation substitutes the factory itself as the key for the adapter.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Adapter adapt(Notifier notifier, Object type) {
		return super.adapt(notifier, this);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object adapt(Object object, Object type) {
		if (isFactoryForType(type)) {
			Object adapter = super.adapt(object, type);
			if (!(type instanceof Class<?>) || (((Class<?>)type).isInstance(adapter))) {
				return adapter;
			}
		}

		return null;
	}

	/**
	 * This adds a listener.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void addListener(INotifyChangedListener notifyChangedListener) {
		changeNotifier.addListener(notifyChangedListener);
	}

	/**
	 * This removes a listener.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void removeListener(INotifyChangedListener notifyChangedListener) {
		changeNotifier.removeListener(notifyChangedListener);
	}

	/**
	 * This delegates to {@link #changeNotifier} and to {@link #parentAdapterFactory}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void fireNotifyChanged(Notification notification) {
		changeNotifier.fireNotifyChanged(notification);

		if (parentAdapterFactory != null) {
			parentAdapterFactory.fireNotifyChanged(notification);
		}
	}

	/**
	 * This disposes all of the item providers created by this factory. 
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void dispose() {
		if (platformCallItemProvider != null) platformCallItemProvider.dispose();
		if (platformCallModuleItemProvider != null) platformCallModuleItemProvider.dispose();
		if (convertSQLDeveloperModelToEcoreItemProvider != null) convertSQLDeveloperModelToEcoreItemProvider.dispose();
		if (importBIRDFromMSAccessItemProvider != null) importBIRDFromMSAccessItemProvider.dispose();
		if (createLogicalTransformationViewForScopeItemProvider != null) createLogicalTransformationViewForScopeItemProvider.dispose();
	}

}
