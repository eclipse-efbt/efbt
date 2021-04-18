/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.data_definition.provider;


import java.util.Collection;
import java.util.List;

import org.eclipse.efbt.cocalimo.smcubes.model.core.provider.SmcubesEditPlugin;

import org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE_ITEM;
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
 * This is the item provider adapter for a {@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE_ITEM} object.
 * <!-- begin-user-doc -->
 * <!-- end-user-doc -->
 * @generated
 */
public class CUBE_STRUCTURE_ITEMItemProvider 
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
	public CUBE_STRUCTURE_ITEMItemProvider(AdapterFactory adapterFactory) {
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

			addAttribute_associated_variablePropertyDescriptor(object);
			addCube_structure_idPropertyDescriptor(object);
			addDescriptionPropertyDescriptor(object);
			addDimension_typePropertyDescriptor(object);
			addIs_flowPropertyDescriptor(object);
			addIs_mandatoryPropertyDescriptor(object);
			addMember_idPropertyDescriptor(object);
			addOrderPropertyDescriptor(object);
			addRolePropertyDescriptor(object);
			addSubdomain_idPropertyDescriptor(object);
			addVariable_idPropertyDescriptor(object);
			addVariable_set_idPropertyDescriptor(object);
			addIsIdentifierPropertyDescriptor(object);
			addCube_variable_codePropertyDescriptor(object);
		}
		return itemPropertyDescriptors;
	}

	/**
	 * This adds a property descriptor for the Attribute associated variable feature.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected void addAttribute_associated_variablePropertyDescriptor(Object object) {
		itemPropertyDescriptors.add
			(createItemPropertyDescriptor
				(((ComposeableAdapterFactory)adapterFactory).getRootAdapterFactory(),
				 getResourceLocator(),
				 getString("_UI_CUBE_STRUCTURE_ITEM_attribute_associated_variable_feature"),
				 getString("_UI_PropertyDescriptor_description", "_UI_CUBE_STRUCTURE_ITEM_attribute_associated_variable_feature", "_UI_CUBE_STRUCTURE_ITEM_type"),
				 Data_definitionPackage.Literals.CUBE_STRUCTURE_ITEM__ATTRIBUTE_ASSOCIATED_VARIABLE,
				 true,
				 false,
				 true,
				 null,
				 null,
				 null));
	}

	/**
	 * This adds a property descriptor for the Cube structure id feature.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected void addCube_structure_idPropertyDescriptor(Object object) {
		itemPropertyDescriptors.add
			(createItemPropertyDescriptor
				(((ComposeableAdapterFactory)adapterFactory).getRootAdapterFactory(),
				 getResourceLocator(),
				 getString("_UI_CUBE_STRUCTURE_ITEM_cube_structure_id_feature"),
				 getString("_UI_PropertyDescriptor_description", "_UI_CUBE_STRUCTURE_ITEM_cube_structure_id_feature", "_UI_CUBE_STRUCTURE_ITEM_type"),
				 Data_definitionPackage.Literals.CUBE_STRUCTURE_ITEM__CUBE_STRUCTURE_ID,
				 true,
				 false,
				 true,
				 null,
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
				 getString("_UI_CUBE_STRUCTURE_ITEM_description_feature"),
				 getString("_UI_PropertyDescriptor_description", "_UI_CUBE_STRUCTURE_ITEM_description_feature", "_UI_CUBE_STRUCTURE_ITEM_type"),
				 Data_definitionPackage.Literals.CUBE_STRUCTURE_ITEM__DESCRIPTION,
				 true,
				 false,
				 false,
				 ItemPropertyDescriptor.GENERIC_VALUE_IMAGE,
				 null,
				 null));
	}

	/**
	 * This adds a property descriptor for the Dimension type feature.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected void addDimension_typePropertyDescriptor(Object object) {
		itemPropertyDescriptors.add
			(createItemPropertyDescriptor
				(((ComposeableAdapterFactory)adapterFactory).getRootAdapterFactory(),
				 getResourceLocator(),
				 getString("_UI_CUBE_STRUCTURE_ITEM_dimension_type_feature"),
				 getString("_UI_PropertyDescriptor_description", "_UI_CUBE_STRUCTURE_ITEM_dimension_type_feature", "_UI_CUBE_STRUCTURE_ITEM_type"),
				 Data_definitionPackage.Literals.CUBE_STRUCTURE_ITEM__DIMENSION_TYPE,
				 true,
				 false,
				 false,
				 ItemPropertyDescriptor.GENERIC_VALUE_IMAGE,
				 null,
				 null));
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
				 getString("_UI_CUBE_STRUCTURE_ITEM_is_flow_feature"),
				 getString("_UI_PropertyDescriptor_description", "_UI_CUBE_STRUCTURE_ITEM_is_flow_feature", "_UI_CUBE_STRUCTURE_ITEM_type"),
				 Data_definitionPackage.Literals.CUBE_STRUCTURE_ITEM__IS_FLOW,
				 true,
				 false,
				 false,
				 ItemPropertyDescriptor.BOOLEAN_VALUE_IMAGE,
				 null,
				 null));
	}

	/**
	 * This adds a property descriptor for the Is mandatory feature.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected void addIs_mandatoryPropertyDescriptor(Object object) {
		itemPropertyDescriptors.add
			(createItemPropertyDescriptor
				(((ComposeableAdapterFactory)adapterFactory).getRootAdapterFactory(),
				 getResourceLocator(),
				 getString("_UI_CUBE_STRUCTURE_ITEM_is_mandatory_feature"),
				 getString("_UI_PropertyDescriptor_description", "_UI_CUBE_STRUCTURE_ITEM_is_mandatory_feature", "_UI_CUBE_STRUCTURE_ITEM_type"),
				 Data_definitionPackage.Literals.CUBE_STRUCTURE_ITEM__IS_MANDATORY,
				 true,
				 false,
				 false,
				 ItemPropertyDescriptor.BOOLEAN_VALUE_IMAGE,
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
				 getString("_UI_CUBE_STRUCTURE_ITEM_member_id_feature"),
				 getString("_UI_PropertyDescriptor_description", "_UI_CUBE_STRUCTURE_ITEM_member_id_feature", "_UI_CUBE_STRUCTURE_ITEM_type"),
				 Data_definitionPackage.Literals.CUBE_STRUCTURE_ITEM__MEMBER_ID,
				 true,
				 false,
				 true,
				 null,
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
				 getString("_UI_CUBE_STRUCTURE_ITEM_order_feature"),
				 getString("_UI_PropertyDescriptor_description", "_UI_CUBE_STRUCTURE_ITEM_order_feature", "_UI_CUBE_STRUCTURE_ITEM_type"),
				 Data_definitionPackage.Literals.CUBE_STRUCTURE_ITEM__ORDER,
				 true,
				 false,
				 false,
				 ItemPropertyDescriptor.INTEGRAL_VALUE_IMAGE,
				 null,
				 null));
	}

	/**
	 * This adds a property descriptor for the Role feature.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected void addRolePropertyDescriptor(Object object) {
		itemPropertyDescriptors.add
			(createItemPropertyDescriptor
				(((ComposeableAdapterFactory)adapterFactory).getRootAdapterFactory(),
				 getResourceLocator(),
				 getString("_UI_CUBE_STRUCTURE_ITEM_role_feature"),
				 getString("_UI_PropertyDescriptor_description", "_UI_CUBE_STRUCTURE_ITEM_role_feature", "_UI_CUBE_STRUCTURE_ITEM_type"),
				 Data_definitionPackage.Literals.CUBE_STRUCTURE_ITEM__ROLE,
				 true,
				 false,
				 false,
				 ItemPropertyDescriptor.GENERIC_VALUE_IMAGE,
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
				 getString("_UI_CUBE_STRUCTURE_ITEM_subdomain_id_feature"),
				 getString("_UI_PropertyDescriptor_description", "_UI_CUBE_STRUCTURE_ITEM_subdomain_id_feature", "_UI_CUBE_STRUCTURE_ITEM_type"),
				 Data_definitionPackage.Literals.CUBE_STRUCTURE_ITEM__SUBDOMAIN_ID,
				 true,
				 false,
				 true,
				 null,
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
				 getString("_UI_CUBE_STRUCTURE_ITEM_variable_id_feature"),
				 getString("_UI_PropertyDescriptor_description", "_UI_CUBE_STRUCTURE_ITEM_variable_id_feature", "_UI_CUBE_STRUCTURE_ITEM_type"),
				 Data_definitionPackage.Literals.CUBE_STRUCTURE_ITEM__VARIABLE_ID,
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
				 getString("_UI_CUBE_STRUCTURE_ITEM_variable_set_id_feature"),
				 getString("_UI_PropertyDescriptor_description", "_UI_CUBE_STRUCTURE_ITEM_variable_set_id_feature", "_UI_CUBE_STRUCTURE_ITEM_type"),
				 Data_definitionPackage.Literals.CUBE_STRUCTURE_ITEM__VARIABLE_SET_ID,
				 true,
				 false,
				 true,
				 null,
				 null,
				 null));
	}

	/**
	 * This adds a property descriptor for the Is Identifier feature.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected void addIsIdentifierPropertyDescriptor(Object object) {
		itemPropertyDescriptors.add
			(createItemPropertyDescriptor
				(((ComposeableAdapterFactory)adapterFactory).getRootAdapterFactory(),
				 getResourceLocator(),
				 getString("_UI_CUBE_STRUCTURE_ITEM_isIdentifier_feature"),
				 getString("_UI_PropertyDescriptor_description", "_UI_CUBE_STRUCTURE_ITEM_isIdentifier_feature", "_UI_CUBE_STRUCTURE_ITEM_type"),
				 Data_definitionPackage.Literals.CUBE_STRUCTURE_ITEM__IS_IDENTIFIER,
				 true,
				 false,
				 false,
				 ItemPropertyDescriptor.BOOLEAN_VALUE_IMAGE,
				 null,
				 null));
	}

	/**
	 * This adds a property descriptor for the Cube variable code feature.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected void addCube_variable_codePropertyDescriptor(Object object) {
		itemPropertyDescriptors.add
			(createItemPropertyDescriptor
				(((ComposeableAdapterFactory)adapterFactory).getRootAdapterFactory(),
				 getResourceLocator(),
				 getString("_UI_CUBE_STRUCTURE_ITEM_cube_variable_code_feature"),
				 getString("_UI_PropertyDescriptor_description", "_UI_CUBE_STRUCTURE_ITEM_cube_variable_code_feature", "_UI_CUBE_STRUCTURE_ITEM_type"),
				 Data_definitionPackage.Literals.CUBE_STRUCTURE_ITEM__CUBE_VARIABLE_CODE,
				 true,
				 false,
				 false,
				 ItemPropertyDescriptor.GENERIC_VALUE_IMAGE,
				 null,
				 null));
	}

	/**
	 * This returns CUBE_STRUCTURE_ITEM.gif.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object getImage(Object object) {
		return overlayImage(object, getResourceLocator().getImage("full/obj16/CUBE_STRUCTURE_ITEM"));
	}

	/**
	 * This returns the label text for the adapted class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public String getText(Object object) {
		String label = ((CUBE_STRUCTURE_ITEM)object).getDescription();
		return label == null || label.length() == 0 ?
			getString("_UI_CUBE_STRUCTURE_ITEM_type") :
			getString("_UI_CUBE_STRUCTURE_ITEM_type") + " " + label;
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

		switch (notification.getFeatureID(CUBE_STRUCTURE_ITEM.class)) {
			case Data_definitionPackage.CUBE_STRUCTURE_ITEM__DESCRIPTION:
			case Data_definitionPackage.CUBE_STRUCTURE_ITEM__DIMENSION_TYPE:
			case Data_definitionPackage.CUBE_STRUCTURE_ITEM__IS_FLOW:
			case Data_definitionPackage.CUBE_STRUCTURE_ITEM__IS_MANDATORY:
			case Data_definitionPackage.CUBE_STRUCTURE_ITEM__ORDER:
			case Data_definitionPackage.CUBE_STRUCTURE_ITEM__ROLE:
			case Data_definitionPackage.CUBE_STRUCTURE_ITEM__IS_IDENTIFIER:
			case Data_definitionPackage.CUBE_STRUCTURE_ITEM__CUBE_VARIABLE_CODE:
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
