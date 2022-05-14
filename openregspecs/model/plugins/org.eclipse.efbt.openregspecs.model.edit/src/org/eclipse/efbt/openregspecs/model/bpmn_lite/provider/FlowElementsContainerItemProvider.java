/**
 */
package org.eclipse.efbt.openregspecs.model.bpmn_lite.provider;


import java.util.Collection;
import java.util.List;

import org.eclipse.efbt.openregspecs.model.bpmn_lite.Bpmn_liteFactory;
import org.eclipse.efbt.openregspecs.model.bpmn_lite.Bpmn_litePackage;
import org.eclipse.efbt.openregspecs.model.bpmn_lite.FlowElementsContainer;

import org.eclipse.emf.common.notify.AdapterFactory;
import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.ecore.EStructuralFeature;

import org.eclipse.emf.edit.provider.IItemPropertyDescriptor;
import org.eclipse.emf.edit.provider.ViewerNotification;

/**
 * This is the item provider adapter for a {@link org.eclipse.efbt.openregspecs.model.bpmn_lite.FlowElementsContainer} object.
 * <!-- begin-user-doc -->
 * <!-- end-user-doc -->
 * @generated
 */
public class FlowElementsContainerItemProvider extends BaseElementItemProvider {
	/**
	 * This constructs an instance from a factory and a notifier.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public FlowElementsContainerItemProvider(AdapterFactory adapterFactory) {
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
			childrenFeatures.add(Bpmn_litePackage.Literals.FLOW_ELEMENTS_CONTAINER__FLOW_ELEMENTS);
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
	 * This returns the label text for the adapted class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public String getText(Object object) {
		String label = ((FlowElementsContainer)object).getId();
		return label == null || label.length() == 0 ?
			getString("_UI_FlowElementsContainer_type") :
			getString("_UI_FlowElementsContainer_type") + " " + label;
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

		switch (notification.getFeatureID(FlowElementsContainer.class)) {
			case Bpmn_litePackage.FLOW_ELEMENTS_CONTAINER__FLOW_ELEMENTS:
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
				(Bpmn_litePackage.Literals.FLOW_ELEMENTS_CONTAINER__FLOW_ELEMENTS,
				 Bpmn_liteFactory.eINSTANCE.createSequenceFlow()));

		newChildDescriptors.add
			(createChildParameter
				(Bpmn_litePackage.Literals.FLOW_ELEMENTS_CONTAINER__FLOW_ELEMENTS,
				 Bpmn_liteFactory.eINSTANCE.createTask()));

		newChildDescriptors.add
			(createChildParameter
				(Bpmn_litePackage.Literals.FLOW_ELEMENTS_CONTAINER__FLOW_ELEMENTS,
				 Bpmn_liteFactory.eINSTANCE.createExclusiveGateway()));

		newChildDescriptors.add
			(createChildParameter
				(Bpmn_litePackage.Literals.FLOW_ELEMENTS_CONTAINER__FLOW_ELEMENTS,
				 Bpmn_liteFactory.eINSTANCE.createInclusiveGateway()));

		newChildDescriptors.add
			(createChildParameter
				(Bpmn_litePackage.Literals.FLOW_ELEMENTS_CONTAINER__FLOW_ELEMENTS,
				 Bpmn_liteFactory.eINSTANCE.createParallelGateway()));

		newChildDescriptors.add
			(createChildParameter
				(Bpmn_litePackage.Literals.FLOW_ELEMENTS_CONTAINER__FLOW_ELEMENTS,
				 Bpmn_liteFactory.eINSTANCE.createServiceTask()));

		newChildDescriptors.add
			(createChildParameter
				(Bpmn_litePackage.Literals.FLOW_ELEMENTS_CONTAINER__FLOW_ELEMENTS,
				 Bpmn_liteFactory.eINSTANCE.createSubProcess()));

		newChildDescriptors.add
			(createChildParameter
				(Bpmn_litePackage.Literals.FLOW_ELEMENTS_CONTAINER__FLOW_ELEMENTS,
				 Bpmn_liteFactory.eINSTANCE.createScriptTask()));

		newChildDescriptors.add
			(createChildParameter
				(Bpmn_litePackage.Literals.FLOW_ELEMENTS_CONTAINER__FLOW_ELEMENTS,
				 Bpmn_liteFactory.eINSTANCE.createUserTask()));
	}

}
