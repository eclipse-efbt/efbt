/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.core.provider;


import java.util.Collection;
import java.util.List;

import org.eclipse.efbt.cocalimo.smcubes.model.core.CorePackage;
import org.eclipse.efbt.cocalimo.smcubes.model.core.MEMBER_HIERARCHY_NODE;

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
 * This is the item provider adapter for a {@link org.eclipse.efbt.cocalimo.smcubes.model.core.MEMBER_HIERARCHY_NODE} object.
 * <!-- begin-user-doc -->
 * <!-- end-user-doc -->
 * @generated
 */
public class MEMBER_HIERARCHY_NODEItemProvider 
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
	public MEMBER_HIERARCHY_NODEItemProvider(AdapterFactory adapterFactory) {
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

			addComparatorPropertyDescriptor(object);
			addLevelPropertyDescriptor(object);
			addMember_hierarchy_idPropertyDescriptor(object);
			addMember_idPropertyDescriptor(object);
			addOperatorPropertyDescriptor(object);
			addParent_member_idPropertyDescriptor(object);
			addValid_fromPropertyDescriptor(object);
			addValid_toPropertyDescriptor(object);
		}
		return itemPropertyDescriptors;
	}

	/**
	 * This adds a property descriptor for the Comparator feature.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected void addComparatorPropertyDescriptor(Object object) {
		itemPropertyDescriptors.add
			(createItemPropertyDescriptor
				(((ComposeableAdapterFactory)adapterFactory).getRootAdapterFactory(),
				 getResourceLocator(),
				 getString("_UI_MEMBER_HIERARCHY_NODE_comparator_feature"),
				 getString("_UI_PropertyDescriptor_description", "_UI_MEMBER_HIERARCHY_NODE_comparator_feature", "_UI_MEMBER_HIERARCHY_NODE_type"),
				 CorePackage.Literals.MEMBER_HIERARCHY_NODE__COMPARATOR,
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
				 getString("_UI_MEMBER_HIERARCHY_NODE_level_feature"),
				 getString("_UI_PropertyDescriptor_description", "_UI_MEMBER_HIERARCHY_NODE_level_feature", "_UI_MEMBER_HIERARCHY_NODE_type"),
				 CorePackage.Literals.MEMBER_HIERARCHY_NODE__LEVEL,
				 true,
				 false,
				 false,
				 ItemPropertyDescriptor.INTEGRAL_VALUE_IMAGE,
				 null,
				 null));
	}

	/**
	 * This adds a property descriptor for the Member hierarchy id feature.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected void addMember_hierarchy_idPropertyDescriptor(Object object) {
		itemPropertyDescriptors.add
			(createItemPropertyDescriptor
				(((ComposeableAdapterFactory)adapterFactory).getRootAdapterFactory(),
				 getResourceLocator(),
				 getString("_UI_MEMBER_HIERARCHY_NODE_member_hierarchy_id_feature"),
				 getString("_UI_PropertyDescriptor_description", "_UI_MEMBER_HIERARCHY_NODE_member_hierarchy_id_feature", "_UI_MEMBER_HIERARCHY_NODE_type"),
				 CorePackage.Literals.MEMBER_HIERARCHY_NODE__MEMBER_HIERARCHY_ID,
				 true,
				 false,
				 true,
				 null,
				 null,
				 null));
	}

	/**
	 * This adds a property descriptor for the Member id feature.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected void addMember_idPropertyDescriptor(Object object) {
		itemPropertyDescriptors.add
			(createItemPropertyDescriptor
				(((ComposeableAdapterFactory)adapterFactory).getRootAdapterFactory(),
				 getResourceLocator(),
				 getString("_UI_MEMBER_HIERARCHY_NODE_member_id_feature"),
				 getString("_UI_PropertyDescriptor_description", "_UI_MEMBER_HIERARCHY_NODE_member_id_feature", "_UI_MEMBER_HIERARCHY_NODE_type"),
				 CorePackage.Literals.MEMBER_HIERARCHY_NODE__MEMBER_ID,
				 true,
				 false,
				 true,
				 null,
				 null,
				 null));
	}

	/**
	 * This adds a property descriptor for the Operator feature.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected void addOperatorPropertyDescriptor(Object object) {
		itemPropertyDescriptors.add
			(createItemPropertyDescriptor
				(((ComposeableAdapterFactory)adapterFactory).getRootAdapterFactory(),
				 getResourceLocator(),
				 getString("_UI_MEMBER_HIERARCHY_NODE_operator_feature"),
				 getString("_UI_PropertyDescriptor_description", "_UI_MEMBER_HIERARCHY_NODE_operator_feature", "_UI_MEMBER_HIERARCHY_NODE_type"),
				 CorePackage.Literals.MEMBER_HIERARCHY_NODE__OPERATOR,
				 true,
				 false,
				 false,
				 ItemPropertyDescriptor.GENERIC_VALUE_IMAGE,
				 null,
				 null));
	}

	/**
	 * This adds a property descriptor for the Parent member id feature.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected void addParent_member_idPropertyDescriptor(Object object) {
		itemPropertyDescriptors.add
			(createItemPropertyDescriptor
				(((ComposeableAdapterFactory)adapterFactory).getRootAdapterFactory(),
				 getResourceLocator(),
				 getString("_UI_MEMBER_HIERARCHY_NODE_parent_member_id_feature"),
				 getString("_UI_PropertyDescriptor_description", "_UI_MEMBER_HIERARCHY_NODE_parent_member_id_feature", "_UI_MEMBER_HIERARCHY_NODE_type"),
				 CorePackage.Literals.MEMBER_HIERARCHY_NODE__PARENT_MEMBER_ID,
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
				 getString("_UI_MEMBER_HIERARCHY_NODE_valid_from_feature"),
				 getString("_UI_PropertyDescriptor_description", "_UI_MEMBER_HIERARCHY_NODE_valid_from_feature", "_UI_MEMBER_HIERARCHY_NODE_type"),
				 CorePackage.Literals.MEMBER_HIERARCHY_NODE__VALID_FROM,
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
				 getString("_UI_MEMBER_HIERARCHY_NODE_valid_to_feature"),
				 getString("_UI_PropertyDescriptor_description", "_UI_MEMBER_HIERARCHY_NODE_valid_to_feature", "_UI_MEMBER_HIERARCHY_NODE_type"),
				 CorePackage.Literals.MEMBER_HIERARCHY_NODE__VALID_TO,
				 true,
				 false,
				 false,
				 ItemPropertyDescriptor.GENERIC_VALUE_IMAGE,
				 null,
				 null));
	}

	/**
	 * This returns MEMBER_HIERARCHY_NODE.gif.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object getImage(Object object) {
		return overlayImage(object, getResourceLocator().getImage("full/obj16/MEMBER_HIERARCHY_NODE"));
	}

	/**
	 * This returns the label text for the adapted class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public String getText(Object object) {
		String label = ((MEMBER_HIERARCHY_NODE)object).getComparator();
		return label == null || label.length() == 0 ?
			getString("_UI_MEMBER_HIERARCHY_NODE_type") :
			getString("_UI_MEMBER_HIERARCHY_NODE_type") + " " + label;
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

		switch (notification.getFeatureID(MEMBER_HIERARCHY_NODE.class)) {
			case CorePackage.MEMBER_HIERARCHY_NODE__COMPARATOR:
			case CorePackage.MEMBER_HIERARCHY_NODE__LEVEL:
			case CorePackage.MEMBER_HIERARCHY_NODE__OPERATOR:
			case CorePackage.MEMBER_HIERARCHY_NODE__VALID_FROM:
			case CorePackage.MEMBER_HIERARCHY_NODE__VALID_TO:
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
