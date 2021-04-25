/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.vtl_transformation.provider;


import java.util.Collection;
import java.util.List;

import org.eclipse.efbt.cocalimo.smcubes.model.core.provider.SmcubesEditPlugin;

import org.eclipse.efbt.cocalimo.smcubes.model.vtl_transformation.TRANSFORMATION_SCHEME;
import org.eclipse.efbt.cocalimo.smcubes.model.vtl_transformation.Vtl_transformationPackage;

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
 * This is the item provider adapter for a {@link org.eclipse.efbt.cocalimo.smcubes.model.vtl_transformation.TRANSFORMATION_SCHEME} object.
 * <!-- begin-user-doc -->
 * <!-- end-user-doc -->
 * @generated
 */
public class TRANSFORMATION_SCHEMEItemProvider 
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
	public TRANSFORMATION_SCHEMEItemProvider(AdapterFactory adapterFactory) {
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

			addTransformation_scheme_idPropertyDescriptor(object);
			addDescriptionPropertyDescriptor(object);
			addNamePropertyDescriptor(object);
			addCodePropertyDescriptor(object);
			addVersionPropertyDescriptor(object);
			addValid_toPropertyDescriptor(object);
			addValid_fromPropertyDescriptor(object);
			addDisplayNamePropertyDescriptor(object);
		}
		return itemPropertyDescriptors;
	}

	/**
	 * This adds a property descriptor for the Transformation scheme id feature.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected void addTransformation_scheme_idPropertyDescriptor(Object object) {
		itemPropertyDescriptors.add
			(createItemPropertyDescriptor
				(((ComposeableAdapterFactory)adapterFactory).getRootAdapterFactory(),
				 getResourceLocator(),
				 getString("_UI_TRANSFORMATION_SCHEME_transformation_scheme_id_feature"),
				 getString("_UI_PropertyDescriptor_description", "_UI_TRANSFORMATION_SCHEME_transformation_scheme_id_feature", "_UI_TRANSFORMATION_SCHEME_type"),
				 Vtl_transformationPackage.Literals.TRANSFORMATION_SCHEME__TRANSFORMATION_SCHEME_ID,
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
				 getString("_UI_TRANSFORMATION_SCHEME_description_feature"),
				 getString("_UI_PropertyDescriptor_description", "_UI_TRANSFORMATION_SCHEME_description_feature", "_UI_TRANSFORMATION_SCHEME_type"),
				 Vtl_transformationPackage.Literals.TRANSFORMATION_SCHEME__DESCRIPTION,
				 true,
				 false,
				 false,
				 ItemPropertyDescriptor.GENERIC_VALUE_IMAGE,
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
				 getString("_UI_TRANSFORMATION_SCHEME_name_feature"),
				 getString("_UI_PropertyDescriptor_description", "_UI_TRANSFORMATION_SCHEME_name_feature", "_UI_TRANSFORMATION_SCHEME_type"),
				 Vtl_transformationPackage.Literals.TRANSFORMATION_SCHEME__NAME,
				 true,
				 false,
				 false,
				 ItemPropertyDescriptor.GENERIC_VALUE_IMAGE,
				 null,
				 null));
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
				 getString("_UI_TRANSFORMATION_SCHEME_code_feature"),
				 getString("_UI_PropertyDescriptor_description", "_UI_TRANSFORMATION_SCHEME_code_feature", "_UI_TRANSFORMATION_SCHEME_type"),
				 Vtl_transformationPackage.Literals.TRANSFORMATION_SCHEME__CODE,
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
				 getString("_UI_TRANSFORMATION_SCHEME_version_feature"),
				 getString("_UI_PropertyDescriptor_description", "_UI_TRANSFORMATION_SCHEME_version_feature", "_UI_TRANSFORMATION_SCHEME_type"),
				 Vtl_transformationPackage.Literals.TRANSFORMATION_SCHEME__VERSION,
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
				 getString("_UI_TRANSFORMATION_SCHEME_valid_to_feature"),
				 getString("_UI_PropertyDescriptor_description", "_UI_TRANSFORMATION_SCHEME_valid_to_feature", "_UI_TRANSFORMATION_SCHEME_type"),
				 Vtl_transformationPackage.Literals.TRANSFORMATION_SCHEME__VALID_TO,
				 true,
				 false,
				 false,
				 ItemPropertyDescriptor.GENERIC_VALUE_IMAGE,
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
				 getString("_UI_TRANSFORMATION_SCHEME_valid_from_feature"),
				 getString("_UI_PropertyDescriptor_description", "_UI_TRANSFORMATION_SCHEME_valid_from_feature", "_UI_TRANSFORMATION_SCHEME_type"),
				 Vtl_transformationPackage.Literals.TRANSFORMATION_SCHEME__VALID_FROM,
				 true,
				 false,
				 false,
				 ItemPropertyDescriptor.GENERIC_VALUE_IMAGE,
				 null,
				 null));
	}

	/**
	 * This adds a property descriptor for the Display Name feature.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected void addDisplayNamePropertyDescriptor(Object object) {
		itemPropertyDescriptors.add
			(createItemPropertyDescriptor
				(((ComposeableAdapterFactory)adapterFactory).getRootAdapterFactory(),
				 getResourceLocator(),
				 getString("_UI_TRANSFORMATION_SCHEME_displayName_feature"),
				 getString("_UI_PropertyDescriptor_description", "_UI_TRANSFORMATION_SCHEME_displayName_feature", "_UI_TRANSFORMATION_SCHEME_type"),
				 Vtl_transformationPackage.Literals.TRANSFORMATION_SCHEME__DISPLAY_NAME,
				 true,
				 false,
				 false,
				 ItemPropertyDescriptor.GENERIC_VALUE_IMAGE,
				 null,
				 null));
	}

	/**
	 * This returns TRANSFORMATION_SCHEME.gif.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object getImage(Object object) {
		return overlayImage(object, getResourceLocator().getImage("full/obj16/TRANSFORMATION_SCHEME"));
	}

	/**
	 * This returns the label text for the adapted class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public String getText(Object object) {
		String label = ((TRANSFORMATION_SCHEME)object).getName();
		return label == null || label.length() == 0 ?
			getString("_UI_TRANSFORMATION_SCHEME_type") :
			getString("_UI_TRANSFORMATION_SCHEME_type") + " " + label;
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

		switch (notification.getFeatureID(TRANSFORMATION_SCHEME.class)) {
			case Vtl_transformationPackage.TRANSFORMATION_SCHEME__TRANSFORMATION_SCHEME_ID:
			case Vtl_transformationPackage.TRANSFORMATION_SCHEME__DESCRIPTION:
			case Vtl_transformationPackage.TRANSFORMATION_SCHEME__NAME:
			case Vtl_transformationPackage.TRANSFORMATION_SCHEME__CODE:
			case Vtl_transformationPackage.TRANSFORMATION_SCHEME__VERSION:
			case Vtl_transformationPackage.TRANSFORMATION_SCHEME__VALID_TO:
			case Vtl_transformationPackage.TRANSFORMATION_SCHEME__VALID_FROM:
			case Vtl_transformationPackage.TRANSFORMATION_SCHEME__DISPLAY_NAME:
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
