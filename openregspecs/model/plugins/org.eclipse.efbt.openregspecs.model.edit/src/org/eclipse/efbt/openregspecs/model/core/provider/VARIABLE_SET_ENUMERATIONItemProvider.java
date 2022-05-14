/**
 */
package org.eclipse.efbt.openregspecs.model.core.provider;


import java.util.Collection;
import java.util.List;

import org.eclipse.efbt.openregspecs.model.core.CorePackage;
import org.eclipse.efbt.openregspecs.model.core.VARIABLE_SET_ENUMERATION;

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
 * This is the item provider adapter for a {@link org.eclipse.efbt.openregspecs.model.core.VARIABLE_SET_ENUMERATION} object.
 * <!-- begin-user-doc -->
 * <!-- end-user-doc -->
 * @generated
 */
public class VARIABLE_SET_ENUMERATIONItemProvider 
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
	public VARIABLE_SET_ENUMERATIONItemProvider(AdapterFactory adapterFactory) {
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

			addIs_flowPropertyDescriptor(object);
			addOrderPropertyDescriptor(object);
			addSubdomain_idPropertyDescriptor(object);
			addValid_fromPropertyDescriptor(object);
			addValid_toPropertyDescriptor(object);
			addVariable_idPropertyDescriptor(object);
			addVariable_set_idPropertyDescriptor(object);
		}
		return itemPropertyDescriptors;
	}

	/**
	 * This adds a property descriptor for the Is flow feature.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected void addIs_flowPropertyDescriptor(Object object) {
		itemPropertyDescriptors.add
			(createItemPropertyDescriptor
				(((ComposeableAdapterFactory)adapterFactory).getRootAdapterFactory(),
				 getResourceLocator(),
				 getString("_UI_VARIABLE_SET_ENUMERATION_is_flow_feature"),
				 getString("_UI_PropertyDescriptor_description", "_UI_VARIABLE_SET_ENUMERATION_is_flow_feature", "_UI_VARIABLE_SET_ENUMERATION_type"),
				 CorePackage.Literals.VARIABLE_SET_ENUMERATION__IS_FLOW,
				 true,
				 false,
				 false,
				 ItemPropertyDescriptor.BOOLEAN_VALUE_IMAGE,
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
				 getString("_UI_VARIABLE_SET_ENUMERATION_order_feature"),
				 getString("_UI_PropertyDescriptor_description", "_UI_VARIABLE_SET_ENUMERATION_order_feature", "_UI_VARIABLE_SET_ENUMERATION_type"),
				 CorePackage.Literals.VARIABLE_SET_ENUMERATION__ORDER,
				 true,
				 false,
				 false,
				 ItemPropertyDescriptor.INTEGRAL_VALUE_IMAGE,
				 null,
				 null));
	}

	/**
	 * This adds a property descriptor for the Subdomain id feature.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected void addSubdomain_idPropertyDescriptor(Object object) {
		itemPropertyDescriptors.add
			(createItemPropertyDescriptor
				(((ComposeableAdapterFactory)adapterFactory).getRootAdapterFactory(),
				 getResourceLocator(),
				 getString("_UI_VARIABLE_SET_ENUMERATION_subdomain_id_feature"),
				 getString("_UI_PropertyDescriptor_description", "_UI_VARIABLE_SET_ENUMERATION_subdomain_id_feature", "_UI_VARIABLE_SET_ENUMERATION_type"),
				 CorePackage.Literals.VARIABLE_SET_ENUMERATION__SUBDOMAIN_ID,
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
				 getString("_UI_VARIABLE_SET_ENUMERATION_valid_from_feature"),
				 getString("_UI_PropertyDescriptor_description", "_UI_VARIABLE_SET_ENUMERATION_valid_from_feature", "_UI_VARIABLE_SET_ENUMERATION_type"),
				 CorePackage.Literals.VARIABLE_SET_ENUMERATION__VALID_FROM,
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
				 getString("_UI_VARIABLE_SET_ENUMERATION_valid_to_feature"),
				 getString("_UI_PropertyDescriptor_description", "_UI_VARIABLE_SET_ENUMERATION_valid_to_feature", "_UI_VARIABLE_SET_ENUMERATION_type"),
				 CorePackage.Literals.VARIABLE_SET_ENUMERATION__VALID_TO,
				 true,
				 false,
				 false,
				 ItemPropertyDescriptor.GENERIC_VALUE_IMAGE,
				 null,
				 null));
	}

	/**
	 * This adds a property descriptor for the Variable id feature.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected void addVariable_idPropertyDescriptor(Object object) {
		itemPropertyDescriptors.add
			(createItemPropertyDescriptor
				(((ComposeableAdapterFactory)adapterFactory).getRootAdapterFactory(),
				 getResourceLocator(),
				 getString("_UI_VARIABLE_SET_ENUMERATION_variable_id_feature"),
				 getString("_UI_PropertyDescriptor_description", "_UI_VARIABLE_SET_ENUMERATION_variable_id_feature", "_UI_VARIABLE_SET_ENUMERATION_type"),
				 CorePackage.Literals.VARIABLE_SET_ENUMERATION__VARIABLE_ID,
				 true,
				 false,
				 true,
				 null,
				 null,
				 null));
	}

	/**
	 * This adds a property descriptor for the Variable set id feature.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected void addVariable_set_idPropertyDescriptor(Object object) {
		itemPropertyDescriptors.add
			(createItemPropertyDescriptor
				(((ComposeableAdapterFactory)adapterFactory).getRootAdapterFactory(),
				 getResourceLocator(),
				 getString("_UI_VARIABLE_SET_ENUMERATION_variable_set_id_feature"),
				 getString("_UI_PropertyDescriptor_description", "_UI_VARIABLE_SET_ENUMERATION_variable_set_id_feature", "_UI_VARIABLE_SET_ENUMERATION_type"),
				 CorePackage.Literals.VARIABLE_SET_ENUMERATION__VARIABLE_SET_ID,
				 true,
				 false,
				 true,
				 null,
				 null,
				 null));
	}

	/**
	 * This returns VARIABLE_SET_ENUMERATION.gif.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object getImage(Object object) {
		return overlayImage(object, getResourceLocator().getImage("full/obj16/VARIABLE_SET_ENUMERATION"));
	}

	/**
	 * This returns the label text for the adapted class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public String getText(Object object) {
		VARIABLE_SET_ENUMERATION variablE_SET_ENUMERATION = (VARIABLE_SET_ENUMERATION)object;
		return getString("_UI_VARIABLE_SET_ENUMERATION_type") + " " + variablE_SET_ENUMERATION.isIs_flow();
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

		switch (notification.getFeatureID(VARIABLE_SET_ENUMERATION.class)) {
			case CorePackage.VARIABLE_SET_ENUMERATION__IS_FLOW:
			case CorePackage.VARIABLE_SET_ENUMERATION__ORDER:
			case CorePackage.VARIABLE_SET_ENUMERATION__VALID_FROM:
			case CorePackage.VARIABLE_SET_ENUMERATION__VALID_TO:
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
