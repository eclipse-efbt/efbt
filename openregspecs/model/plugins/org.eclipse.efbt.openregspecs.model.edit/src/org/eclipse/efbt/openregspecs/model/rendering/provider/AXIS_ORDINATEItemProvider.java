/**
 */
package org.eclipse.efbt.openregspecs.model.rendering.provider;


import java.util.Collection;
import java.util.List;

import org.eclipse.efbt.openregspecs.model.core.provider.Smcubes_modelEditPlugin;

import org.eclipse.efbt.openregspecs.model.rendering.AXIS_ORDINATE;
import org.eclipse.efbt.openregspecs.model.rendering.RenderingPackage;

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
 * This is the item provider adapter for a {@link org.eclipse.efbt.openregspecs.model.rendering.AXIS_ORDINATE} object.
 * <!-- begin-user-doc -->
 * <!-- end-user-doc -->
 * @generated
 */
public class AXIS_ORDINATEItemProvider 
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
	public AXIS_ORDINATEItemProvider(AdapterFactory adapterFactory) {
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

			addAxis_idPropertyDescriptor(object);
			addAxis_ordinate_idPropertyDescriptor(object);
			addCodePropertyDescriptor(object);
			addDescriptionPropertyDescriptor(object);
			addIs_abstract_headerPropertyDescriptor(object);
			addLevelPropertyDescriptor(object);
			addNamePropertyDescriptor(object);
			addOrderPropertyDescriptor(object);
			addParent_axis_ordinate_idPropertyDescriptor(object);
			addPathPropertyDescriptor(object);
		}
		return itemPropertyDescriptors;
	}

	/**
	 * This adds a property descriptor for the Axis id feature.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected void addAxis_idPropertyDescriptor(Object object) {
		itemPropertyDescriptors.add
			(createItemPropertyDescriptor
				(((ComposeableAdapterFactory)adapterFactory).getRootAdapterFactory(),
				 getResourceLocator(),
				 getString("_UI_AXIS_ORDINATE_axis_id_feature"),
				 getString("_UI_PropertyDescriptor_description", "_UI_AXIS_ORDINATE_axis_id_feature", "_UI_AXIS_ORDINATE_type"),
				 RenderingPackage.Literals.AXIS_ORDINATE__AXIS_ID,
				 true,
				 false,
				 true,
				 null,
				 null,
				 null));
	}

	/**
	 * This adds a property descriptor for the Axis ordinate id feature.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected void addAxis_ordinate_idPropertyDescriptor(Object object) {
		itemPropertyDescriptors.add
			(createItemPropertyDescriptor
				(((ComposeableAdapterFactory)adapterFactory).getRootAdapterFactory(),
				 getResourceLocator(),
				 getString("_UI_AXIS_ORDINATE_axis_ordinate_id_feature"),
				 getString("_UI_PropertyDescriptor_description", "_UI_AXIS_ORDINATE_axis_ordinate_id_feature", "_UI_AXIS_ORDINATE_type"),
				 RenderingPackage.Literals.AXIS_ORDINATE__AXIS_ORDINATE_ID,
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
				 getString("_UI_AXIS_ORDINATE_code_feature"),
				 getString("_UI_PropertyDescriptor_description", "_UI_AXIS_ORDINATE_code_feature", "_UI_AXIS_ORDINATE_type"),
				 RenderingPackage.Literals.AXIS_ORDINATE__CODE,
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
				 getString("_UI_AXIS_ORDINATE_description_feature"),
				 getString("_UI_PropertyDescriptor_description", "_UI_AXIS_ORDINATE_description_feature", "_UI_AXIS_ORDINATE_type"),
				 RenderingPackage.Literals.AXIS_ORDINATE__DESCRIPTION,
				 true,
				 false,
				 false,
				 ItemPropertyDescriptor.GENERIC_VALUE_IMAGE,
				 null,
				 null));
	}

	/**
	 * This adds a property descriptor for the Is abstract header feature.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected void addIs_abstract_headerPropertyDescriptor(Object object) {
		itemPropertyDescriptors.add
			(createItemPropertyDescriptor
				(((ComposeableAdapterFactory)adapterFactory).getRootAdapterFactory(),
				 getResourceLocator(),
				 getString("_UI_AXIS_ORDINATE_is_abstract_header_feature"),
				 getString("_UI_PropertyDescriptor_description", "_UI_AXIS_ORDINATE_is_abstract_header_feature", "_UI_AXIS_ORDINATE_type"),
				 RenderingPackage.Literals.AXIS_ORDINATE__IS_ABSTRACT_HEADER,
				 true,
				 false,
				 false,
				 ItemPropertyDescriptor.GENERIC_VALUE_IMAGE,
				 null,
				 null));
	}

	/**
	 * This adds a property descriptor for the Level feature.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected void addLevelPropertyDescriptor(Object object) {
		itemPropertyDescriptors.add
			(createItemPropertyDescriptor
				(((ComposeableAdapterFactory)adapterFactory).getRootAdapterFactory(),
				 getResourceLocator(),
				 getString("_UI_AXIS_ORDINATE_level_feature"),
				 getString("_UI_PropertyDescriptor_description", "_UI_AXIS_ORDINATE_level_feature", "_UI_AXIS_ORDINATE_type"),
				 RenderingPackage.Literals.AXIS_ORDINATE__LEVEL,
				 true,
				 false,
				 false,
				 ItemPropertyDescriptor.INTEGRAL_VALUE_IMAGE,
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
				 getString("_UI_AXIS_ORDINATE_name_feature"),
				 getString("_UI_PropertyDescriptor_description", "_UI_AXIS_ORDINATE_name_feature", "_UI_AXIS_ORDINATE_type"),
				 RenderingPackage.Literals.AXIS_ORDINATE__NAME,
				 true,
				 false,
				 false,
				 ItemPropertyDescriptor.GENERIC_VALUE_IMAGE,
				 null,
				 null));
	}

	/**
	 * This adds a property descriptor for the Order feature.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected void addOrderPropertyDescriptor(Object object) {
		itemPropertyDescriptors.add
			(createItemPropertyDescriptor
				(((ComposeableAdapterFactory)adapterFactory).getRootAdapterFactory(),
				 getResourceLocator(),
				 getString("_UI_AXIS_ORDINATE_order_feature"),
				 getString("_UI_PropertyDescriptor_description", "_UI_AXIS_ORDINATE_order_feature", "_UI_AXIS_ORDINATE_type"),
				 RenderingPackage.Literals.AXIS_ORDINATE__ORDER,
				 true,
				 false,
				 false,
				 ItemPropertyDescriptor.INTEGRAL_VALUE_IMAGE,
				 null,
				 null));
	}

	/**
	 * This adds a property descriptor for the Parent axis ordinate id feature.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected void addParent_axis_ordinate_idPropertyDescriptor(Object object) {
		itemPropertyDescriptors.add
			(createItemPropertyDescriptor
				(((ComposeableAdapterFactory)adapterFactory).getRootAdapterFactory(),
				 getResourceLocator(),
				 getString("_UI_AXIS_ORDINATE_parent_axis_ordinate_id_feature"),
				 getString("_UI_PropertyDescriptor_description", "_UI_AXIS_ORDINATE_parent_axis_ordinate_id_feature", "_UI_AXIS_ORDINATE_type"),
				 RenderingPackage.Literals.AXIS_ORDINATE__PARENT_AXIS_ORDINATE_ID,
				 true,
				 false,
				 true,
				 null,
				 null,
				 null));
	}

	/**
	 * This adds a property descriptor for the Path feature.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected void addPathPropertyDescriptor(Object object) {
		itemPropertyDescriptors.add
			(createItemPropertyDescriptor
				(((ComposeableAdapterFactory)adapterFactory).getRootAdapterFactory(),
				 getResourceLocator(),
				 getString("_UI_AXIS_ORDINATE_path_feature"),
				 getString("_UI_PropertyDescriptor_description", "_UI_AXIS_ORDINATE_path_feature", "_UI_AXIS_ORDINATE_type"),
				 RenderingPackage.Literals.AXIS_ORDINATE__PATH,
				 true,
				 false,
				 false,
				 ItemPropertyDescriptor.GENERIC_VALUE_IMAGE,
				 null,
				 null));
	}

	/**
	 * This returns AXIS_ORDINATE.gif.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object getImage(Object object) {
		return overlayImage(object, getResourceLocator().getImage("full/obj16/AXIS_ORDINATE"));
	}

	/**
	 * This returns the label text for the adapted class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public String getText(Object object) {
		String label = ((AXIS_ORDINATE)object).getName();
		return label == null || label.length() == 0 ?
			getString("_UI_AXIS_ORDINATE_type") :
			getString("_UI_AXIS_ORDINATE_type") + " " + label;
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

		switch (notification.getFeatureID(AXIS_ORDINATE.class)) {
			case RenderingPackage.AXIS_ORDINATE__AXIS_ORDINATE_ID:
			case RenderingPackage.AXIS_ORDINATE__CODE:
			case RenderingPackage.AXIS_ORDINATE__DESCRIPTION:
			case RenderingPackage.AXIS_ORDINATE__IS_ABSTRACT_HEADER:
			case RenderingPackage.AXIS_ORDINATE__LEVEL:
			case RenderingPackage.AXIS_ORDINATE__NAME:
			case RenderingPackage.AXIS_ORDINATE__ORDER:
			case RenderingPackage.AXIS_ORDINATE__PATH:
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
		return Smcubes_modelEditPlugin.INSTANCE;
	}

}
