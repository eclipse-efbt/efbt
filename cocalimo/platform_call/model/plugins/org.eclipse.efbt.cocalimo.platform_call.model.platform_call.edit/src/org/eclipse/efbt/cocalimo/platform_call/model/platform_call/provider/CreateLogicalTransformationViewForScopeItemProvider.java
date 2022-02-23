/**
 */
package org.eclipse.efbt.cocalimo.platform_call.model.platform_call.provider;


import java.util.Collection;
import java.util.List;
import org.eclipse.efbt.cocalimo.platform_call.model.platform_call.CreateLogicalTransformationViewForScope;
import org.eclipse.efbt.cocalimo.platform_call.model.platform_call.Platform_callPackage;

import org.eclipse.emf.common.notify.AdapterFactory;
import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.ecore.EStructuralFeature;

import org.eclipse.emf.edit.provider.ComposeableAdapterFactory;
import org.eclipse.emf.edit.provider.IItemPropertyDescriptor;
import org.eclipse.emf.edit.provider.ViewerNotification;

/**
 * This is the item provider adapter for a {@link org.eclipse.efbt.cocalimo.platform_call.model.platform_call.CreateLogicalTransformationViewForScope} object.
 * <!-- begin-user-doc -->
 * <!-- end-user-doc -->
 * @generated
 */
public class CreateLogicalTransformationViewForScopeItemProvider extends PlatformCallItemProvider {
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

			addLogicalTransformationModulePropertyDescriptor(object);
			addAttributeLineagePropertyDescriptor(object);
		}
		return itemPropertyDescriptors;
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
	 * This adds a property descriptor for the Attribute Lineage feature.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected void addAttributeLineagePropertyDescriptor(Object object) {
		itemPropertyDescriptors.add
			(createItemPropertyDescriptor
				(((ComposeableAdapterFactory)adapterFactory).getRootAdapterFactory(),
				 getResourceLocator(),
				 getString("_UI_CreateLogicalTransformationViewForScope_attributeLineage_feature"),
				 getString("_UI_PropertyDescriptor_description", "_UI_CreateLogicalTransformationViewForScope_attributeLineage_feature", "_UI_CreateLogicalTransformationViewForScope_type"),
				 Platform_callPackage.Literals.CREATE_LOGICAL_TRANSFORMATION_VIEW_FOR_SCOPE__ATTRIBUTE_LINEAGE,
				 true,
				 false,
				 true,
				 null,
				 null,
				 null));
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
			childrenFeatures.add(Platform_callPackage.Literals.CREATE_LOGICAL_TRANSFORMATION_VIEW_FOR_SCOPE__TEST_SCOPE);
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
			case Platform_callPackage.CREATE_LOGICAL_TRANSFORMATION_VIEW_FOR_SCOPE__TEST_SCOPE:
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
				(Platform_callPackage.Literals.CREATE_LOGICAL_TRANSFORMATION_VIEW_FOR_SCOPE__TEST_SCOPE,
				 org.eclipse.efbt.cocalimo.logical_transformations.model.logical_transformations.Logical_transformationsFactory.eINSTANCE.createE2ETestScope()));
	}

}
