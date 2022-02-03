/**
 */
package org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.provider;


import java.util.Collection;
import java.util.List;

import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.Ecore_plus_logicFactory;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.Ecore_plus_logicPackage;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.RowCreationApproachForEntity;

import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_structure.provider.Ecore_plusEditPlugin;

import org.eclipse.emf.common.notify.AdapterFactory;
import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.common.util.ResourceLocator;

import org.eclipse.emf.ecore.EStructuralFeature;

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
 * This is the item provider adapter for a {@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.RowCreationApproachForEntity} object.
 * <!-- begin-user-doc -->
 * <!-- end-user-doc -->
 * @generated
 */
public class RowCreationApproachForEntityItemProvider 
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
	public RowCreationApproachForEntityItemProvider(AdapterFactory adapterFactory) {
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

			addEntityPropertyDescriptor(object);
			addNamePropertyDescriptor(object);
		}
		return itemPropertyDescriptors;
	}

	/**
	 * This adds a property descriptor for the Entity feature.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected void addEntityPropertyDescriptor(Object object) {
		itemPropertyDescriptors.add
			(createItemPropertyDescriptor
				(((ComposeableAdapterFactory)adapterFactory).getRootAdapterFactory(),
				 getResourceLocator(),
				 getString("_UI_RowCreationApproachForEntity_entity_feature"),
				 getString("_UI_PropertyDescriptor_description", "_UI_RowCreationApproachForEntity_entity_feature", "_UI_RowCreationApproachForEntity_type"),
				 Ecore_plus_logicPackage.Literals.ROW_CREATION_APPROACH_FOR_ENTITY__ENTITY,
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
				 getString("_UI_RowCreationApproachForEntity_name_feature"),
				 getString("_UI_PropertyDescriptor_description", "_UI_RowCreationApproachForEntity_name_feature", "_UI_RowCreationApproachForEntity_type"),
				 Ecore_plus_logicPackage.Literals.ROW_CREATION_APPROACH_FOR_ENTITY__NAME,
				 true,
				 false,
				 false,
				 ItemPropertyDescriptor.GENERIC_VALUE_IMAGE,
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
			childrenFeatures.add(Ecore_plus_logicPackage.Literals.ROW_CREATION_APPROACH_FOR_ENTITY__ROW_CREATION_APPROACH);
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
	 * This returns RowCreationApproachForEntity.gif.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object getImage(Object object) {
		return overlayImage(object, getResourceLocator().getImage("full/obj16/RowCreationApproachForEntity"));
	}

	/**
	 * This returns the label text for the adapted class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public String getText(Object object) {
		String label = ((RowCreationApproachForEntity)object).getName();
		return label == null || label.length() == 0 ?
			getString("_UI_RowCreationApproachForEntity_type") :
			getString("_UI_RowCreationApproachForEntity_type") + " " + label;
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

		switch (notification.getFeatureID(RowCreationApproachForEntity.class)) {
			case Ecore_plus_logicPackage.ROW_CREATION_APPROACH_FOR_ENTITY__NAME:
				fireNotifyChanged(new ViewerNotification(notification, notification.getNotifier(), false, true));
				return;
			case Ecore_plus_logicPackage.ROW_CREATION_APPROACH_FOR_ENTITY__ROW_CREATION_APPROACH:
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
				(Ecore_plus_logicPackage.Literals.ROW_CREATION_APPROACH_FOR_ENTITY__ROW_CREATION_APPROACH,
				 Ecore_plus_logicFactory.eINSTANCE.createRowCreationApproach()));

		newChildDescriptors.add
			(createChildParameter
				(Ecore_plus_logicPackage.Literals.ROW_CREATION_APPROACH_FOR_ENTITY__ROW_CREATION_APPROACH,
				 Ecore_plus_logicFactory.eINSTANCE.createCustomRowCreationApproach()));

		newChildDescriptors.add
			(createChildParameter
				(Ecore_plus_logicPackage.Literals.ROW_CREATION_APPROACH_FOR_ENTITY__ROW_CREATION_APPROACH,
				 Ecore_plus_logicFactory.eINSTANCE.createFilterAndGroupToOneRowCreationApproach()));

		newChildDescriptors.add
			(createChildParameter
				(Ecore_plus_logicPackage.Literals.ROW_CREATION_APPROACH_FOR_ENTITY__ROW_CREATION_APPROACH,
				 Ecore_plus_logicFactory.eINSTANCE.createOneToOneRowCreationApproach()));

		newChildDescriptors.add
			(createChildParameter
				(Ecore_plus_logicPackage.Literals.ROW_CREATION_APPROACH_FOR_ENTITY__ROW_CREATION_APPROACH,
				 Ecore_plus_logicFactory.eINSTANCE.createFilterRowCreationApproach()));

		newChildDescriptors.add
			(createChildParameter
				(Ecore_plus_logicPackage.Literals.ROW_CREATION_APPROACH_FOR_ENTITY__ROW_CREATION_APPROACH,
				 Ecore_plus_logicFactory.eINSTANCE.createGroupByRowCreationApproach()));

		newChildDescriptors.add
			(createChildParameter
				(Ecore_plus_logicPackage.Literals.ROW_CREATION_APPROACH_FOR_ENTITY__ROW_CREATION_APPROACH,
				 Ecore_plus_logicFactory.eINSTANCE.createRowJoinFunction()));

		newChildDescriptors.add
			(createChildParameter
				(Ecore_plus_logicPackage.Literals.ROW_CREATION_APPROACH_FOR_ENTITY__ROW_CREATION_APPROACH,
				 Ecore_plus_logicFactory.eINSTANCE.createUnionRowCreationApproach()));

		newChildDescriptors.add
			(createChildParameter
				(Ecore_plus_logicPackage.Literals.ROW_CREATION_APPROACH_FOR_ENTITY__ROW_CREATION_APPROACH,
				 Ecore_plus_logicFactory.eINSTANCE.createExplodeArrayOfStructsRowFunction()));
	}

	/**
	 * Return the resource locator for this item provider's resources.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public ResourceLocator getResourceLocator() {
		return Ecore_plusEditPlugin.INSTANCE;
	}

}
