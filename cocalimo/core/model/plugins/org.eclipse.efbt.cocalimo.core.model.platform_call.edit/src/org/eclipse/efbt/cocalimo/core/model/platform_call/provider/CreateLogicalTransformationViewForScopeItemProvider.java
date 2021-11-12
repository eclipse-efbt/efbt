/**
 */
package org.eclipse.efbt.cocalimo.core.model.platform_call.provider;


import java.util.Collection;
import java.util.List;

import org.eclipse.efbt.cocalimo.core.model.platform_call.CreateLogicalTransformationViewForScope;
import org.eclipse.efbt.cocalimo.core.model.platform_call.Platform_callPackage;

import org.eclipse.emf.common.notify.AdapterFactory;
import org.eclipse.emf.common.notify.Notification;
import org.eclipse.emf.edit.provider.ComposeableAdapterFactory;
import org.eclipse.emf.edit.provider.IItemPropertyDescriptor;
import org.eclipse.emf.edit.provider.ItemPropertyDescriptor;
import org.eclipse.emf.edit.provider.ViewerNotification;

/**
 * This is the item provider adapter for a {@link org.eclipse.efbt.cocalimo.core.model.platform_call.CreateLogicalTransformationViewForScope} object.
 * <!-- begin-user-doc -->
 * <!-- end-user-doc -->
 * @generated
 */
public class CreateLogicalTransformationViewForScopeItemProvider 
	extends PlatformCallItemProvider {
	/**
	 * This constructs an instance from a factory and a notifier.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public CreateLogicalTransformationViewForScopeItemProvider(AdapterFactory adapterFactory) {
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

			addScriptTasksInScopePropertyDescriptor(object);
			addScenariosOutOfScopePropertyDescriptor(object);
			addLogicalTransformationModulePropertyDescriptor(object);
			addKeepLayoutPropertyDescriptor(object);
		}
		return itemPropertyDescriptors;
	}

	/**
	 * This adds a property descriptor for the Script Tasks In Scope feature.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected void addScriptTasksInScopePropertyDescriptor(Object object) {
		itemPropertyDescriptors.add
			(createItemPropertyDescriptor
				(((ComposeableAdapterFactory)adapterFactory).getRootAdapterFactory(),
				 getResourceLocator(),
				 getString("_UI_CreateLogicalTransformationViewForScope_scriptTasksInScope_feature"),
				 getString("_UI_PropertyDescriptor_description", "_UI_CreateLogicalTransformationViewForScope_scriptTasksInScope_feature", "_UI_CreateLogicalTransformationViewForScope_type"),
				 Platform_callPackage.Literals.CREATE_LOGICAL_TRANSFORMATION_VIEW_FOR_SCOPE__SCRIPT_TASKS_IN_SCOPE,
				 true,
				 false,
				 true,
				 null,
				 null,
				 null));
	}

	/**
	 * This adds a property descriptor for the Scenarios Out Of Scope feature.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected void addScenariosOutOfScopePropertyDescriptor(Object object) {
		itemPropertyDescriptors.add
			(createItemPropertyDescriptor
				(((ComposeableAdapterFactory)adapterFactory).getRootAdapterFactory(),
				 getResourceLocator(),
				 getString("_UI_CreateLogicalTransformationViewForScope_scenariosOutOfScope_feature"),
				 getString("_UI_PropertyDescriptor_description", "_UI_CreateLogicalTransformationViewForScope_scenariosOutOfScope_feature", "_UI_CreateLogicalTransformationViewForScope_type"),
				 Platform_callPackage.Literals.CREATE_LOGICAL_TRANSFORMATION_VIEW_FOR_SCOPE__SCENARIOS_OUT_OF_SCOPE,
				 true,
				 false,
				 true,
				 null,
				 null,
				 null));
	}

	/**
	 * This adds a property descriptor for the Logical Transformation Module feature.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected void addLogicalTransformationModulePropertyDescriptor(Object object) {
		itemPropertyDescriptors.add
			(createItemPropertyDescriptor
				(((ComposeableAdapterFactory)adapterFactory).getRootAdapterFactory(),
				 getResourceLocator(),
				 getString("_UI_CreateLogicalTransformationViewForScope_logicalTransformationModule_feature"),
				 getString("_UI_PropertyDescriptor_description", "_UI_CreateLogicalTransformationViewForScope_logicalTransformationModule_feature", "_UI_CreateLogicalTransformationViewForScope_type"),
				 Platform_callPackage.Literals.CREATE_LOGICAL_TRANSFORMATION_VIEW_FOR_SCOPE__LOGICAL_TRANSFORMATION_MODULE,
				 true,
				 false,
				 true,
				 null,
				 null,
				 null));
	}

	/**
	 * This adds a property descriptor for the Keep Layout feature.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected void addKeepLayoutPropertyDescriptor(Object object) {
		itemPropertyDescriptors.add
			(createItemPropertyDescriptor
				(((ComposeableAdapterFactory)adapterFactory).getRootAdapterFactory(),
				 getResourceLocator(),
				 getString("_UI_CreateLogicalTransformationViewForScope_keepLayout_feature"),
				 getString("_UI_PropertyDescriptor_description", "_UI_CreateLogicalTransformationViewForScope_keepLayout_feature", "_UI_CreateLogicalTransformationViewForScope_type"),
				 Platform_callPackage.Literals.CREATE_LOGICAL_TRANSFORMATION_VIEW_FOR_SCOPE__KEEP_LAYOUT,
				 true,
				 false,
				 false,
				 ItemPropertyDescriptor.BOOLEAN_VALUE_IMAGE,
				 null,
				 null));
	}

	/**
	 * This returns CreateLogicalTransformationViewForScope.gif.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object getImage(Object object) {
		return overlayImage(object, getResourceLocator().getImage("full/obj16/CreateLogicalTransformationViewForScope"));
	}

	/**
	 * This returns the label text for the adapted class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public String getText(Object object) {
		String label = ((CreateLogicalTransformationViewForScope)object).getName();
		return label == null || label.length() == 0 ?
			getString("_UI_CreateLogicalTransformationViewForScope_type") :
			getString("_UI_CreateLogicalTransformationViewForScope_type") + " " + label;
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

		switch (notification.getFeatureID(CreateLogicalTransformationViewForScope.class)) {
			case Platform_callPackage.CREATE_LOGICAL_TRANSFORMATION_VIEW_FOR_SCOPE__KEEP_LAYOUT:
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

}
