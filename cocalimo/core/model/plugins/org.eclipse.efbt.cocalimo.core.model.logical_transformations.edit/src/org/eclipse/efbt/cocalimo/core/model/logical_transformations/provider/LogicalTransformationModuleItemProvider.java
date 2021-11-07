/**
 */
package org.eclipse.efbt.cocalimo.core.model.logical_transformations.provider;


import java.util.Collection;
import java.util.List;

import org.eclipse.efbt.cocalimo.core.model.bpmn_lite.Bpmn_liteFactory;

import org.eclipse.efbt.cocalimo.core.model.logical_transformations.LogicalTransformationModule;
import org.eclipse.efbt.cocalimo.core.model.logical_transformations.Logical_transformationsFactory;
import org.eclipse.efbt.cocalimo.core.model.logical_transformations.Logical_transformationsPackage;

import org.eclipse.efbt.cocalimo.core.model.module_management.provider.ModuleItemProvider;

import org.eclipse.efbt.cocalimo.core.model.requirements_text.Requirements_textFactory;
import org.eclipse.emf.common.notify.AdapterFactory;
import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.common.util.ResourceLocator;

import org.eclipse.emf.ecore.EStructuralFeature;

import org.eclipse.emf.edit.provider.IItemPropertyDescriptor;
import org.eclipse.emf.edit.provider.ViewerNotification;

/**
 * This is the item provider adapter for a {@link org.eclipse.efbt.cocalimo.core.model.logical_transformations.LogicalTransformationModule} object.
 * <!-- begin-user-doc -->
 * <!-- end-user-doc -->
 * @generated
 */
public class LogicalTransformationModuleItemProvider extends ModuleItemProvider {
	/**
	 * This constructs an instance from a factory and a notifier.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public LogicalTransformationModuleItemProvider(AdapterFactory adapterFactory) {
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
			childrenFeatures.add(Logical_transformationsPackage.Literals.LOGICAL_TRANSFORMATION_MODULE__TASK_TAGS);
			childrenFeatures.add(Logical_transformationsPackage.Literals.LOGICAL_TRANSFORMATION_MODULE__SCENARIO_TAGS);
			childrenFeatures.add(Logical_transformationsPackage.Literals.LOGICAL_TRANSFORMATION_MODULE__SUB_PROCESSES);
			childrenFeatures.add(Logical_transformationsPackage.Literals.LOGICAL_TRANSFORMATION_MODULE__REQUIREMENTS);
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
	 * This returns LogicalTransformationModule.gif.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object getImage(Object object) {
		return overlayImage(object, getResourceLocator().getImage("full/obj16/LogicalTransformationModule"));
	}

	/**
	 * This returns the label text for the adapted class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public String getText(Object object) {
		String label = ((LogicalTransformationModule)object).getName();
		return label == null || label.length() == 0 ?
			getString("_UI_LogicalTransformationModule_type") :
			getString("_UI_LogicalTransformationModule_type") + " " + label;
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

		switch (notification.getFeatureID(LogicalTransformationModule.class)) {
			case Logical_transformationsPackage.LOGICAL_TRANSFORMATION_MODULE__TASK_TAGS:
			case Logical_transformationsPackage.LOGICAL_TRANSFORMATION_MODULE__SCENARIO_TAGS:
			case Logical_transformationsPackage.LOGICAL_TRANSFORMATION_MODULE__SUB_PROCESSES:
			case Logical_transformationsPackage.LOGICAL_TRANSFORMATION_MODULE__REQUIREMENTS:
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
				(Logical_transformationsPackage.Literals.LOGICAL_TRANSFORMATION_MODULE__TASK_TAGS,
				 Logical_transformationsFactory.eINSTANCE.createTaskTag()));

		newChildDescriptors.add
			(createChildParameter
				(Logical_transformationsPackage.Literals.LOGICAL_TRANSFORMATION_MODULE__SCENARIO_TAGS,
				 Logical_transformationsFactory.eINSTANCE.createScenarioTag()));

		newChildDescriptors.add
			(createChildParameter
				(Logical_transformationsPackage.Literals.LOGICAL_TRANSFORMATION_MODULE__SUB_PROCESSES,
				 Bpmn_liteFactory.eINSTANCE.createSubProcess()));

		newChildDescriptors.add
			(createChildParameter
				(Logical_transformationsPackage.Literals.LOGICAL_TRANSFORMATION_MODULE__REQUIREMENTS,
				 Requirements_textFactory.eINSTANCE.createRequirementsModule()));
	}

	/**
	 * Return the resource locator for this item provider's resources.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public ResourceLocator getResourceLocator() {
		return TaskEditPlugin.INSTANCE;
	}

}
