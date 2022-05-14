/**
 */
package org.eclipse.efbt.openregspecs.model.data_definition.provider;


import java.util.Collection;
import java.util.List;

import org.eclipse.efbt.openregspecs.model.core.provider.Smcubes_modelEditPlugin;

import org.eclipse.efbt.openregspecs.model.data_definition.CUBE_HIERARCHY_NODE;
import org.eclipse.efbt.openregspecs.model.data_definition.Data_definitionPackage;

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
 * This is the item provider adapter for a {@link org.eclipse.efbt.openregspecs.model.data_definition.CUBE_HIERARCHY_NODE} object.
 * <!-- begin-user-doc -->
 * <!-- end-user-doc -->
 * @generated
 */
public class CUBE_HIERARCHY_NODEItemProvider 
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
	public CUBE_HIERARCHY_NODEItemProvider(AdapterFactory adapterFactory) {
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

			addCube_group_idPropertyDescriptor(object);
			addCube_hierarchy_idPropertyDescriptor(object);
			addLevelPropertyDescriptor(object);
			addNODE_CODEPropertyDescriptor(object);
			addNode_namePropertyDescriptor(object);
			addOrderPropertyDescriptor(object);
			addParent_node_codePropertyDescriptor(object);
			addValid_fromPropertyDescriptor(object);
			addValid_toPropertyDescriptor(object);
		}
		return itemPropertyDescriptors;
	}

	/**
	 * This adds a property descriptor for the Cube group id feature.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected void addCube_group_idPropertyDescriptor(Object object) {
		itemPropertyDescriptors.add
			(createItemPropertyDescriptor
				(((ComposeableAdapterFactory)adapterFactory).getRootAdapterFactory(),
				 getResourceLocator(),
				 getString("_UI_CUBE_HIERARCHY_NODE_cube_group_id_feature"),
				 getString("_UI_PropertyDescriptor_description", "_UI_CUBE_HIERARCHY_NODE_cube_group_id_feature", "_UI_CUBE_HIERARCHY_NODE_type"),
				 Data_definitionPackage.Literals.CUBE_HIERARCHY_NODE__CUBE_GROUP_ID,
				 true,
				 false,
				 true,
				 null,
				 null,
				 null));
	}

	/**
	 * This adds a property descriptor for the Cube hierarchy id feature.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected void addCube_hierarchy_idPropertyDescriptor(Object object) {
		itemPropertyDescriptors.add
			(createItemPropertyDescriptor
				(((ComposeableAdapterFactory)adapterFactory).getRootAdapterFactory(),
				 getResourceLocator(),
				 getString("_UI_CUBE_HIERARCHY_NODE_cube_hierarchy_id_feature"),
				 getString("_UI_PropertyDescriptor_description", "_UI_CUBE_HIERARCHY_NODE_cube_hierarchy_id_feature", "_UI_CUBE_HIERARCHY_NODE_type"),
				 Data_definitionPackage.Literals.CUBE_HIERARCHY_NODE__CUBE_HIERARCHY_ID,
				 true,
				 false,
				 true,
				 null,
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
				 getString("_UI_CUBE_HIERARCHY_NODE_level_feature"),
				 getString("_UI_PropertyDescriptor_description", "_UI_CUBE_HIERARCHY_NODE_level_feature", "_UI_CUBE_HIERARCHY_NODE_type"),
				 Data_definitionPackage.Literals.CUBE_HIERARCHY_NODE__LEVEL,
				 true,
				 false,
				 false,
				 ItemPropertyDescriptor.INTEGRAL_VALUE_IMAGE,
				 null,
				 null));
	}

	/**
	 * This adds a property descriptor for the NODE CODE feature.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected void addNODE_CODEPropertyDescriptor(Object object) {
		itemPropertyDescriptors.add
			(createItemPropertyDescriptor
				(((ComposeableAdapterFactory)adapterFactory).getRootAdapterFactory(),
				 getResourceLocator(),
				 getString("_UI_CUBE_HIERARCHY_NODE_nODE_CODE_feature"),
				 getString("_UI_PropertyDescriptor_description", "_UI_CUBE_HIERARCHY_NODE_nODE_CODE_feature", "_UI_CUBE_HIERARCHY_NODE_type"),
				 Data_definitionPackage.Literals.CUBE_HIERARCHY_NODE__NODE_CODE,
				 true,
				 false,
				 false,
				 ItemPropertyDescriptor.GENERIC_VALUE_IMAGE,
				 null,
				 null));
	}

	/**
	 * This adds a property descriptor for the Node name feature.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected void addNode_namePropertyDescriptor(Object object) {
		itemPropertyDescriptors.add
			(createItemPropertyDescriptor
				(((ComposeableAdapterFactory)adapterFactory).getRootAdapterFactory(),
				 getResourceLocator(),
				 getString("_UI_CUBE_HIERARCHY_NODE_node_name_feature"),
				 getString("_UI_PropertyDescriptor_description", "_UI_CUBE_HIERARCHY_NODE_node_name_feature", "_UI_CUBE_HIERARCHY_NODE_type"),
				 Data_definitionPackage.Literals.CUBE_HIERARCHY_NODE__NODE_NAME,
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
				 getString("_UI_CUBE_HIERARCHY_NODE_order_feature"),
				 getString("_UI_PropertyDescriptor_description", "_UI_CUBE_HIERARCHY_NODE_order_feature", "_UI_CUBE_HIERARCHY_NODE_type"),
				 Data_definitionPackage.Literals.CUBE_HIERARCHY_NODE__ORDER,
				 true,
				 false,
				 false,
				 ItemPropertyDescriptor.INTEGRAL_VALUE_IMAGE,
				 null,
				 null));
	}

	/**
	 * This adds a property descriptor for the Parent node code feature.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected void addParent_node_codePropertyDescriptor(Object object) {
		itemPropertyDescriptors.add
			(createItemPropertyDescriptor
				(((ComposeableAdapterFactory)adapterFactory).getRootAdapterFactory(),
				 getResourceLocator(),
				 getString("_UI_CUBE_HIERARCHY_NODE_parent_node_code_feature"),
				 getString("_UI_PropertyDescriptor_description", "_UI_CUBE_HIERARCHY_NODE_parent_node_code_feature", "_UI_CUBE_HIERARCHY_NODE_type"),
				 Data_definitionPackage.Literals.CUBE_HIERARCHY_NODE__PARENT_NODE_CODE,
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
				 getString("_UI_CUBE_HIERARCHY_NODE_valid_from_feature"),
				 getString("_UI_PropertyDescriptor_description", "_UI_CUBE_HIERARCHY_NODE_valid_from_feature", "_UI_CUBE_HIERARCHY_NODE_type"),
				 Data_definitionPackage.Literals.CUBE_HIERARCHY_NODE__VALID_FROM,
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
				 getString("_UI_CUBE_HIERARCHY_NODE_valid_to_feature"),
				 getString("_UI_PropertyDescriptor_description", "_UI_CUBE_HIERARCHY_NODE_valid_to_feature", "_UI_CUBE_HIERARCHY_NODE_type"),
				 Data_definitionPackage.Literals.CUBE_HIERARCHY_NODE__VALID_TO,
				 true,
				 false,
				 false,
				 ItemPropertyDescriptor.GENERIC_VALUE_IMAGE,
				 null,
				 null));
	}

	/**
	 * This returns CUBE_HIERARCHY_NODE.gif.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object getImage(Object object) {
		return overlayImage(object, getResourceLocator().getImage("full/obj16/CUBE_HIERARCHY_NODE"));
	}

	/**
	 * This returns the label text for the adapted class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public String getText(Object object) {
		String label = ((CUBE_HIERARCHY_NODE)object).getNode_name();
		return label == null || label.length() == 0 ?
			getString("_UI_CUBE_HIERARCHY_NODE_type") :
			getString("_UI_CUBE_HIERARCHY_NODE_type") + " " + label;
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

		switch (notification.getFeatureID(CUBE_HIERARCHY_NODE.class)) {
			case Data_definitionPackage.CUBE_HIERARCHY_NODE__LEVEL:
			case Data_definitionPackage.CUBE_HIERARCHY_NODE__NODE_CODE:
			case Data_definitionPackage.CUBE_HIERARCHY_NODE__NODE_NAME:
			case Data_definitionPackage.CUBE_HIERARCHY_NODE__ORDER:
			case Data_definitionPackage.CUBE_HIERARCHY_NODE__VALID_FROM:
			case Data_definitionPackage.CUBE_HIERARCHY_NODE__VALID_TO:
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
