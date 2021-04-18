/**
 */
package org.eclipse.efbt.language.dtm.model.domain_model_mapping.provider;


import java.util.Collection;
import java.util.List;

import org.eclipse.efbt.cocalimo.core.model.module_management.provider.ModuleItemProvider;

import org.eclipse.efbt.language.dtm.model.domain_model_mapping.DomainMappingModule;
import org.eclipse.efbt.language.dtm.model.domain_model_mapping.Domain_model_mappingFactory;
import org.eclipse.efbt.language.dtm.model.domain_model_mapping.Domain_model_mappingPackage;

import org.eclipse.emf.common.notify.AdapterFactory;
import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.common.util.ResourceLocator;

import org.eclipse.emf.ecore.EStructuralFeature;

import org.eclipse.emf.edit.provider.IItemPropertyDescriptor;
import org.eclipse.emf.edit.provider.ViewerNotification;

/**
 * This is the item provider adapter for a {@link org.eclipse.efbt.language.dtm.model.domain_model_mapping.DomainMappingModule} object.
 * <!-- begin-user-doc -->
 * <!-- end-user-doc -->
 * @generated
 */
public class DomainMappingModuleItemProvider extends ModuleItemProvider {
	/**
	 * This constructs an instance from a factory and a notifier.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public DomainMappingModuleItemProvider(AdapterFactory adapterFactory) {
		super(adapterFactory);
	}

	/**
	 * This returns the property descriptors for the adapted class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public List<IItemPropertyDescriptor> getPropertyDescriptors(Object object) {
		if (itemPropertyDescriptors == null) {
			super.getPropertyDescriptors(object);

		}
		return itemPropertyDescriptors;
	}

	/**
	 * This specifies how to implement {@link #getChildren} and is used to deduce an appropriate feature for an
	 * {@link org.eclipse.emf.edit.command.AddCommand}, {@link org.eclipse.emf.edit.command.RemoveCommand} or
	 * {@link org.eclipse.emf.edit.command.MoveCommand} in {@link #createCommand}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Collection<? extends EStructuralFeature> getChildrenFeatures(Object object) {
		if (childrenFeatures == null) {
			super.getChildrenFeatures(object);
			childrenFeatures.add(Domain_model_mappingPackage.Literals.DOMAIN_MAPPING_MODULE__LEAF_MODULE_TO_VERSIONED_SQL_VIEWS_MODULES);
			childrenFeatures.add(Domain_model_mappingPackage.Literals.DOMAIN_MAPPING_MODULE__LEAF_MODULE_TO_VERSIONED_CUBE_SCHEMA_MODULES);
			childrenFeatures.add(Domain_model_mappingPackage.Literals.DOMAIN_MAPPING_MODULE__LEAF_MODULE_TO_REPORT_CELL_VIEW_MODULES);
			childrenFeatures.add(Domain_model_mappingPackage.Literals.DOMAIN_MAPPING_MODULE__SCENARIO_TO_CODE_CONDITIONS);
		}
		return childrenFeatures;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EStructuralFeature getChildFeature(Object object, Object child) {
		// Check the type of the specified child object and return the proper feature to use for
		// adding (see {@link AddCommand}) it as a child.

		return super.getChildFeature(object, child);
	}

	/**
	 * This returns DomainMappingModule.gif.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object getImage(Object object) {
		return overlayImage(object, getResourceLocator().getImage("full/obj16/DomainMappingModule"));
	}

	/**
	 * This returns the label text for the adapted class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public String getText(Object object) {
		String label = ((DomainMappingModule)object).getName();
		return label == null || label.length() == 0 ?
			getString("_UI_DomainMappingModule_type") :
			getString("_UI_DomainMappingModule_type") + " " + label;
	}


	/**
	 * This handles model notifications by calling {@link #updateChildren} to update any cached
	 * children and by creating a viewer notification, which it passes to {@link #fireNotifyChanged}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void notifyChanged(Notification notification) {
		updateChildren(notification);

		switch (notification.getFeatureID(DomainMappingModule.class)) {
			case Domain_model_mappingPackage.DOMAIN_MAPPING_MODULE__LEAF_MODULE_TO_VERSIONED_SQL_VIEWS_MODULES:
			case Domain_model_mappingPackage.DOMAIN_MAPPING_MODULE__LEAF_MODULE_TO_VERSIONED_CUBE_SCHEMA_MODULES:
			case Domain_model_mappingPackage.DOMAIN_MAPPING_MODULE__LEAF_MODULE_TO_REPORT_CELL_VIEW_MODULES:
			case Domain_model_mappingPackage.DOMAIN_MAPPING_MODULE__SCENARIO_TO_CODE_CONDITIONS:
				fireNotifyChanged(new ViewerNotification(notification, notification.getNotifier(), true, false));
				return;
		}
		super.notifyChanged(notification);
	}

	/**
	 * This adds {@link org.eclipse.emf.edit.command.CommandParameter}s describing the children
	 * that can be created under this object.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected void collectNewChildDescriptors(Collection<Object> newChildDescriptors, Object object) {
		super.collectNewChildDescriptors(newChildDescriptors, object);

		newChildDescriptors.add
			(createChildParameter
				(Domain_model_mappingPackage.Literals.DOMAIN_MAPPING_MODULE__LEAF_MODULE_TO_VERSIONED_SQL_VIEWS_MODULES,
				 Domain_model_mappingFactory.eINSTANCE.createLeafModuleToVersionedSQLViewsModule()));

		newChildDescriptors.add
			(createChildParameter
				(Domain_model_mappingPackage.Literals.DOMAIN_MAPPING_MODULE__LEAF_MODULE_TO_VERSIONED_CUBE_SCHEMA_MODULES,
				 Domain_model_mappingFactory.eINSTANCE.createLeafModuleToVersionedCubeSchemaModule()));

		newChildDescriptors.add
			(createChildParameter
				(Domain_model_mappingPackage.Literals.DOMAIN_MAPPING_MODULE__LEAF_MODULE_TO_REPORT_CELL_VIEW_MODULES,
				 Domain_model_mappingFactory.eINSTANCE.createLeafModuleToReportCellViewModule()));

		newChildDescriptors.add
			(createChildParameter
				(Domain_model_mappingPackage.Literals.DOMAIN_MAPPING_MODULE__SCENARIO_TO_CODE_CONDITIONS,
				 Domain_model_mappingFactory.eINSTANCE.createScenarioToCodeConditions()));
	}

	/**
	 * Return the resource locator for this item provider's resources.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public ResourceLocator getResourceLocator() {
		return dtmEditPlugin.INSTANCE;
	}

}
