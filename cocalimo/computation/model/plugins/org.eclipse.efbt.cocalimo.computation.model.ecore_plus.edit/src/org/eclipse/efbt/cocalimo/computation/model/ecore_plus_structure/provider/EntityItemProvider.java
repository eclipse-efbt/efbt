/**
 */
package org.eclipse.efbt.cocalimo.computation.model.ecore_plus_structure.provider;


import java.util.Collection;
import java.util.List;

import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.Ecore_plus_logicFactory;

import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_structure.Ecore_plus_structureFactory;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_structure.Entity;

import org.eclipse.emf.common.notify.AdapterFactory;
import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.common.util.ResourceLocator;

import org.eclipse.emf.ecore.EcorePackage;

import org.eclipse.emf.ecore.provider.EClassItemProvider;

import org.eclipse.emf.edit.provider.IItemPropertyDescriptor;

/**
 * This is the item provider adapter for a {@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_structure.Entity} object.
 * <!-- begin-user-doc -->
 * <!-- end-user-doc -->
 * @generated
 */
public class EntityItemProvider extends EClassItemProvider {
	/**
	 * This constructs an instance from a factory and a notifier.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EntityItemProvider(AdapterFactory adapterFactory) {
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

		}
		return itemPropertyDescriptors;
	}

	/**
	 * This returns Entity.gif.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object getImage(Object object) {
		return overlayImage(object, getResourceLocator().getImage("full/obj16/Entity"));
	}

	/**
	 * This returns the label text for the adapted class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public String getText(Object object) {
		String label = ((Entity)object).getName();
		return label == null || label.length() == 0 ?
			getString("_UI_Entity_type") :
			getString("_UI_Entity_type") + " " + label;
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
				(EcorePackage.Literals.ECLASS__EOPERATIONS,
				 Ecore_plus_logicFactory.eINSTANCE.createOperation()));

		newChildDescriptors.add
			(createChildParameter
				(EcorePackage.Literals.ECLASS__EOPERATIONS,
				 Ecore_plus_logicFactory.eINSTANCE.createBasicOperation()));

		newChildDescriptors.add
			(createChildParameter
				(EcorePackage.Literals.ECLASS__EOPERATIONS,
				 Ecore_plus_logicFactory.eINSTANCE.createBooleanOperation()));

		newChildDescriptors.add
			(createChildParameter
				(EcorePackage.Literals.ECLASS__EOPERATIONS,
				 Ecore_plus_logicFactory.eINSTANCE.createBasicScenarioOperation()));

		newChildDescriptors.add
			(createChildParameter
				(EcorePackage.Literals.ECLASS__EOPERATIONS,
				 Ecore_plus_logicFactory.eINSTANCE.createRowCreationApproach()));

		newChildDescriptors.add
			(createChildParameter
				(EcorePackage.Literals.ECLASS__EOPERATIONS,
				 Ecore_plus_logicFactory.eINSTANCE.createCustomRowCreationApproach()));

		newChildDescriptors.add
			(createChildParameter
				(EcorePackage.Literals.ECLASS__EOPERATIONS,
				 Ecore_plus_logicFactory.eINSTANCE.createFilterAndGroupToOneRowCreationApproach()));

		newChildDescriptors.add
			(createChildParameter
				(EcorePackage.Literals.ECLASS__EOPERATIONS,
				 Ecore_plus_logicFactory.eINSTANCE.createOneToOneRowCreationApproach()));

		newChildDescriptors.add
			(createChildParameter
				(EcorePackage.Literals.ECLASS__EOPERATIONS,
				 Ecore_plus_logicFactory.eINSTANCE.createFilterRowCreationApproach()));

		newChildDescriptors.add
			(createChildParameter
				(EcorePackage.Literals.ECLASS__EOPERATIONS,
				 Ecore_plus_logicFactory.eINSTANCE.createGroupByRowCreationApproach()));

		newChildDescriptors.add
			(createChildParameter
				(EcorePackage.Literals.ECLASS__EOPERATIONS,
				 Ecore_plus_logicFactory.eINSTANCE.createRowJoinFunction()));

		newChildDescriptors.add
			(createChildParameter
				(EcorePackage.Literals.ECLASS__EOPERATIONS,
				 Ecore_plus_logicFactory.eINSTANCE.createUnionRowCreationApproach()));

		newChildDescriptors.add
			(createChildParameter
				(EcorePackage.Literals.ECLASS__EOPERATIONS,
				 Ecore_plus_logicFactory.eINSTANCE.createExplodeArrayOfStructsRowFunction()));

		newChildDescriptors.add
			(createChildParameter
				(EcorePackage.Literals.ECLASS__EOPERATIONS,
				 Ecore_plus_logicFactory.eINSTANCE.createAggregateOperation()));

		newChildDescriptors.add
			(createChildParameter
				(EcorePackage.Literals.ECLASS__EOPERATIONS,
				 Ecore_plus_logicFactory.eINSTANCE.createEntityFromSourceOperation()));

		newChildDescriptors.add
			(createChildParameter
				(EcorePackage.Literals.ECLASS__EOPERATIONS,
				 Ecore_plus_logicFactory.eINSTANCE.createAttributeFromEntityOperation()));

		newChildDescriptors.add
			(createChildParameter
				(EcorePackage.Literals.ECLASS__EOPERATIONS,
				 Ecore_plus_logicFactory.eINSTANCE.createBasicOperationScenarioSet()));

		newChildDescriptors.add
			(createChildParameter
				(EcorePackage.Literals.ECLASS__ESTRUCTURAL_FEATURES,
				 Ecore_plus_structureFactory.eINSTANCE.createBaseEntityFeature()));
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
