/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.data_definition.provider;


import java.util.Collection;
import java.util.List;

import org.eclipse.efbt.cocalimo.smcubes.model.core.provider.SmcubesEditPlugin;

import org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_RELATIONSHIP;
import org.eclipse.efbt.cocalimo.smcubes.model.data_definition.Data_definitionPackage;

import org.eclipse.emf.common.notify.AdapterFactory;
import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.common.util.ResourceLocator;

import org.eclipse.emf.edit.provider.ComposeableAdapterFactory;
import org.eclipse.emf.edit.provider.IEditingDomainItemProvider;
import org.eclipse.emf.edit.provider.IItemLabelProvider;
import org.eclipse.emf.edit.provider.IItemPropertyDescriptor;
import org.eclipse.emf.edit.provider.IItemPropertySource;
import org.eclipse.emf.edit.provider.IStructuredItemContentProvider;
import org.eclipse.emf.edit.provider.ITreeItemContentProvider;
import org.eclipse.emf.edit.provider.ItemPropertyDescriptor;
import org.eclipse.emf.edit.provider.ItemProviderAdapter;
import org.eclipse.emf.edit.provider.ViewerNotification;

/**
 * This is the item provider adapter for a {@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_RELATIONSHIP} object.
 * <!-- begin-user-doc -->
 * <!-- end-user-doc -->
 * @generated
 */
public class CUBE_RELATIONSHIPItemProvider 
	extends ItemProviderAdapter
	implements
		IEditingDomainItemProvider,
		IStructuredItemContentProvider,
		ITreeItemContentProvider,
		IItemLabelProvider,
		IItemPropertySource {
	/**
	 * This constructs an instance from a factory and a notifier.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public CUBE_RELATIONSHIPItemProvider(AdapterFactory adapterFactory) {
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

			addCodePropertyDescriptor(object);
			addCube_relationship_idPropertyDescriptor(object);
			addDescriptionPropertyDescriptor(object);
			addEstablishes_integrityPropertyDescriptor(object);
			addForeign_cube_idPropertyDescriptor(object);
			addForeign_cube_variable_codePropertyDescriptor(object);
			addMaintenance_agency_idPropertyDescriptor(object);
			addNamePropertyDescriptor(object);
			addPrimary_cube_idPropertyDescriptor(object);
			addPrimary_cube_variable_idPropertyDescriptor(object);
			addValid_fromPropertyDescriptor(object);
			addValid_toPropertyDescriptor(object);
			addVersionPropertyDescriptor(object);
		}
		return itemPropertyDescriptors;
	}

	/**
	 * This adds a property descriptor for the Code feature.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected void addCodePropertyDescriptor(Object object) {
		itemPropertyDescriptors.add
			(createItemPropertyDescriptor
				(((ComposeableAdapterFactory)adapterFactory).getRootAdapterFactory(),
				 getResourceLocator(),
				 getString("_UI_CUBE_RELATIONSHIP_code_feature"),
				 getString("_UI_PropertyDescriptor_description", "_UI_CUBE_RELATIONSHIP_code_feature", "_UI_CUBE_RELATIONSHIP_type"),
				 Data_definitionPackage.Literals.CUBE_RELATIONSHIP__CODE,
				 true,
				 false,
				 false,
				 ItemPropertyDescriptor.GENERIC_VALUE_IMAGE,
				 null,
				 null));
	}

	/**
	 * This adds a property descriptor for the Cube relationship id feature.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected void addCube_relationship_idPropertyDescriptor(Object object) {
		itemPropertyDescriptors.add
			(createItemPropertyDescriptor
				(((ComposeableAdapterFactory)adapterFactory).getRootAdapterFactory(),
				 getResourceLocator(),
				 getString("_UI_CUBE_RELATIONSHIP_cube_relationship_id_feature"),
				 getString("_UI_PropertyDescriptor_description", "_UI_CUBE_RELATIONSHIP_cube_relationship_id_feature", "_UI_CUBE_RELATIONSHIP_type"),
				 Data_definitionPackage.Literals.CUBE_RELATIONSHIP__CUBE_RELATIONSHIP_ID,
				 true,
				 false,
				 false,
				 ItemPropertyDescriptor.GENERIC_VALUE_IMAGE,
				 null,
				 null));
	}

	/**
	 * This adds a property descriptor for the Description feature.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected void addDescriptionPropertyDescriptor(Object object) {
		itemPropertyDescriptors.add
			(createItemPropertyDescriptor
				(((ComposeableAdapterFactory)adapterFactory).getRootAdapterFactory(),
				 getResourceLocator(),
				 getString("_UI_CUBE_RELATIONSHIP_description_feature"),
				 getString("_UI_PropertyDescriptor_description", "_UI_CUBE_RELATIONSHIP_description_feature", "_UI_CUBE_RELATIONSHIP_type"),
				 Data_definitionPackage.Literals.CUBE_RELATIONSHIP__DESCRIPTION,
				 true,
				 false,
				 false,
				 ItemPropertyDescriptor.GENERIC_VALUE_IMAGE,
				 null,
				 null));
	}

	/**
	 * This adds a property descriptor for the Establishes integrity feature.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected void addEstablishes_integrityPropertyDescriptor(Object object) {
		itemPropertyDescriptors.add
			(createItemPropertyDescriptor
				(((ComposeableAdapterFactory)adapterFactory).getRootAdapterFactory(),
				 getResourceLocator(),
				 getString("_UI_CUBE_RELATIONSHIP_establishes_integrity_feature"),
				 getString("_UI_PropertyDescriptor_description", "_UI_CUBE_RELATIONSHIP_establishes_integrity_feature", "_UI_CUBE_RELATIONSHIP_type"),
				 Data_definitionPackage.Literals.CUBE_RELATIONSHIP__ESTABLISHES_INTEGRITY,
				 true,
				 false,
				 false,
				 ItemPropertyDescriptor.BOOLEAN_VALUE_IMAGE,
				 null,
				 null));
	}

	/**
	 * This adds a property descriptor for the Foreign cube id feature.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected void addForeign_cube_idPropertyDescriptor(Object object) {
		itemPropertyDescriptors.add
			(createItemPropertyDescriptor
				(((ComposeableAdapterFactory)adapterFactory).getRootAdapterFactory(),
				 getResourceLocator(),
				 getString("_UI_CUBE_RELATIONSHIP_foreign_cube_id_feature"),
				 getString("_UI_PropertyDescriptor_description", "_UI_CUBE_RELATIONSHIP_foreign_cube_id_feature", "_UI_CUBE_RELATIONSHIP_type"),
				 Data_definitionPackage.Literals.CUBE_RELATIONSHIP__FOREIGN_CUBE_ID,
				 true,
				 false,
				 true,
				 null,
				 null,
				 null));
	}

	/**
	 * This adds a property descriptor for the Foreign cube variable code feature.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected void addForeign_cube_variable_codePropertyDescriptor(Object object) {
		itemPropertyDescriptors.add
			(createItemPropertyDescriptor
				(((ComposeableAdapterFactory)adapterFactory).getRootAdapterFactory(),
				 getResourceLocator(),
				 getString("_UI_CUBE_RELATIONSHIP_foreign_cube_variable_code_feature"),
				 getString("_UI_PropertyDescriptor_description", "_UI_CUBE_RELATIONSHIP_foreign_cube_variable_code_feature", "_UI_CUBE_RELATIONSHIP_type"),
				 Data_definitionPackage.Literals.CUBE_RELATIONSHIP__FOREIGN_CUBE_VARIABLE_CODE,
				 true,
				 false,
				 true,
				 null,
				 null,
				 null));
	}

	/**
	 * This adds a property descriptor for the Maintenance agency id feature.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected void addMaintenance_agency_idPropertyDescriptor(Object object) {
		itemPropertyDescriptors.add
			(createItemPropertyDescriptor
				(((ComposeableAdapterFactory)adapterFactory).getRootAdapterFactory(),
				 getResourceLocator(),
				 getString("_UI_CUBE_RELATIONSHIP_maintenance_agency_id_feature"),
				 getString("_UI_PropertyDescriptor_description", "_UI_CUBE_RELATIONSHIP_maintenance_agency_id_feature", "_UI_CUBE_RELATIONSHIP_type"),
				 Data_definitionPackage.Literals.CUBE_RELATIONSHIP__MAINTENANCE_AGENCY_ID,
				 true,
				 false,
				 true,
				 null,
				 null,
				 null));
	}

	/**
	 * This adds a property descriptor for the Name feature.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected void addNamePropertyDescriptor(Object object) {
		itemPropertyDescriptors.add
			(createItemPropertyDescriptor
				(((ComposeableAdapterFactory)adapterFactory).getRootAdapterFactory(),
				 getResourceLocator(),
				 getString("_UI_CUBE_RELATIONSHIP_name_feature"),
				 getString("_UI_PropertyDescriptor_description", "_UI_CUBE_RELATIONSHIP_name_feature", "_UI_CUBE_RELATIONSHIP_type"),
				 Data_definitionPackage.Literals.CUBE_RELATIONSHIP__NAME,
				 true,
				 false,
				 false,
				 ItemPropertyDescriptor.GENERIC_VALUE_IMAGE,
				 null,
				 null));
	}

	/**
	 * This adds a property descriptor for the Primary cube id feature.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected void addPrimary_cube_idPropertyDescriptor(Object object) {
		itemPropertyDescriptors.add
			(createItemPropertyDescriptor
				(((ComposeableAdapterFactory)adapterFactory).getRootAdapterFactory(),
				 getResourceLocator(),
				 getString("_UI_CUBE_RELATIONSHIP_primary_cube_id_feature"),
				 getString("_UI_PropertyDescriptor_description", "_UI_CUBE_RELATIONSHIP_primary_cube_id_feature", "_UI_CUBE_RELATIONSHIP_type"),
				 Data_definitionPackage.Literals.CUBE_RELATIONSHIP__PRIMARY_CUBE_ID,
				 true,
				 false,
				 true,
				 null,
				 null,
				 null));
	}

	/**
	 * This adds a property descriptor for the Primary cube variable id feature.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected void addPrimary_cube_variable_idPropertyDescriptor(Object object) {
		itemPropertyDescriptors.add
			(createItemPropertyDescriptor
				(((ComposeableAdapterFactory)adapterFactory).getRootAdapterFactory(),
				 getResourceLocator(),
				 getString("_UI_CUBE_RELATIONSHIP_primary_cube_variable_id_feature"),
				 getString("_UI_PropertyDescriptor_description", "_UI_CUBE_RELATIONSHIP_primary_cube_variable_id_feature", "_UI_CUBE_RELATIONSHIP_type"),
				 Data_definitionPackage.Literals.CUBE_RELATIONSHIP__PRIMARY_CUBE_VARIABLE_ID,
				 true,
				 false,
				 true,
				 null,
				 null,
				 null));
	}

	/**
	 * This adds a property descriptor for the Valid from feature.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected void addValid_fromPropertyDescriptor(Object object) {
		itemPropertyDescriptors.add
			(createItemPropertyDescriptor
				(((ComposeableAdapterFactory)adapterFactory).getRootAdapterFactory(),
				 getResourceLocator(),
				 getString("_UI_CUBE_RELATIONSHIP_valid_from_feature"),
				 getString("_UI_PropertyDescriptor_description", "_UI_CUBE_RELATIONSHIP_valid_from_feature", "_UI_CUBE_RELATIONSHIP_type"),
				 Data_definitionPackage.Literals.CUBE_RELATIONSHIP__VALID_FROM,
				 true,
				 false,
				 false,
				 ItemPropertyDescriptor.GENERIC_VALUE_IMAGE,
				 null,
				 null));
	}

	/**
	 * This adds a property descriptor for the Valid to feature.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected void addValid_toPropertyDescriptor(Object object) {
		itemPropertyDescriptors.add
			(createItemPropertyDescriptor
				(((ComposeableAdapterFactory)adapterFactory).getRootAdapterFactory(),
				 getResourceLocator(),
				 getString("_UI_CUBE_RELATIONSHIP_valid_to_feature"),
				 getString("_UI_PropertyDescriptor_description", "_UI_CUBE_RELATIONSHIP_valid_to_feature", "_UI_CUBE_RELATIONSHIP_type"),
				 Data_definitionPackage.Literals.CUBE_RELATIONSHIP__VALID_TO,
				 true,
				 false,
				 false,
				 ItemPropertyDescriptor.GENERIC_VALUE_IMAGE,
				 null,
				 null));
	}

	/**
	 * This adds a property descriptor for the Version feature.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected void addVersionPropertyDescriptor(Object object) {
		itemPropertyDescriptors.add
			(createItemPropertyDescriptor
				(((ComposeableAdapterFactory)adapterFactory).getRootAdapterFactory(),
				 getResourceLocator(),
				 getString("_UI_CUBE_RELATIONSHIP_version_feature"),
				 getString("_UI_PropertyDescriptor_description", "_UI_CUBE_RELATIONSHIP_version_feature", "_UI_CUBE_RELATIONSHIP_type"),
				 Data_definitionPackage.Literals.CUBE_RELATIONSHIP__VERSION,
				 true,
				 false,
				 false,
				 ItemPropertyDescriptor.GENERIC_VALUE_IMAGE,
				 null,
				 null));
	}

	/**
	 * This returns CUBE_RELATIONSHIP.gif.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object getImage(Object object) {
		return overlayImage(object, getResourceLocator().getImage("full/obj16/CUBE_RELATIONSHIP"));
	}

	/**
	 * This returns the label text for the adapted class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public String getText(Object object) {
		String label = ((CUBE_RELATIONSHIP)object).getName();
		return label == null || label.length() == 0 ?
			getString("_UI_CUBE_RELATIONSHIP_type") :
			getString("_UI_CUBE_RELATIONSHIP_type") + " " + label;
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

		switch (notification.getFeatureID(CUBE_RELATIONSHIP.class)) {
			case Data_definitionPackage.CUBE_RELATIONSHIP__CODE:
			case Data_definitionPackage.CUBE_RELATIONSHIP__CUBE_RELATIONSHIP_ID:
			case Data_definitionPackage.CUBE_RELATIONSHIP__DESCRIPTION:
			case Data_definitionPackage.CUBE_RELATIONSHIP__ESTABLISHES_INTEGRITY:
			case Data_definitionPackage.CUBE_RELATIONSHIP__NAME:
			case Data_definitionPackage.CUBE_RELATIONSHIP__VALID_FROM:
			case Data_definitionPackage.CUBE_RELATIONSHIP__VALID_TO:
			case Data_definitionPackage.CUBE_RELATIONSHIP__VERSION:
				fireNotifyChanged(new ViewerNotification(notification, notification.getNotifier(), false, true));
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
	}

	/**
	 * Return the resource locator for this item provider's resources.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public ResourceLocator getResourceLocator() {
		return SmcubesEditPlugin.INSTANCE;
	}

}
