/**
 */
package org.eclipse.efbt.cocalimo.core.model.task.provider;


import java.util.Collection;
import java.util.List;

import org.eclipse.efbt.cocalimo.core.model.task.TaskFactory;
import org.eclipse.efbt.cocalimo.core.model.task.TaskPackage;
import org.eclipse.efbt.cocalimo.core.model.task.TaskSet;

import org.eclipse.emf.common.notify.AdapterFactory;
import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.ecore.EStructuralFeature;

import org.eclipse.emf.edit.provider.IItemPropertyDescriptor;
import org.eclipse.emf.edit.provider.ViewerNotification;

/**
 * This is the item provider adapter for a {@link org.eclipse.efbt.cocalimo.core.model.task.TaskSet} object.
 * <!-- begin-user-doc -->
 * <!-- end-user-doc -->
 * @generated
 */
public class TaskSetItemProvider extends TaskItemProvider {
	/**
	 * This constructs an instance from a factory and a notifier.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public TaskSetItemProvider(AdapterFactory adapterFactory) {
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
			childrenFeatures.add(TaskPackage.Literals.TASK_SET__SUB_TASKS);
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
	 * This returns TaskSet.gif.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object getImage(Object object) {
		return overlayImage(object, getResourceLocator().getImage("full/obj16/TaskSet"));
	}

	/**
	 * This returns the label text for the adapted class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public String getText(Object object) {
		String label = ((TaskSet)object).getName();
		return label == null || label.length() == 0 ?
			getString("_UI_TaskSet_type") :
			getString("_UI_TaskSet_type") + " " + label;
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

		switch (notification.getFeatureID(TaskSet.class)) {
			case TaskPackage.TASK_SET__SUB_TASKS:
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
				(TaskPackage.Literals.TASK_SET__SUB_TASKS,
				 TaskFactory.eINSTANCE.createLeafTask()));

		newChildDescriptors.add
			(createChildParameter
				(TaskPackage.Literals.TASK_SET__SUB_TASKS,
				 TaskFactory.eINSTANCE.createDataProcessingTask()));

		newChildDescriptors.add
			(createChildParameter
				(TaskPackage.Literals.TASK_SET__SUB_TASKS,
				 TaskFactory.eINSTANCE.createTaskSet()));

		newChildDescriptors.add
			(createChildParameter
				(TaskPackage.Literals.TASK_SET__SUB_TASKS,
				 TaskFactory.eINSTANCE.createParallelTask()));

		newChildDescriptors.add
			(createChildParameter
				(TaskPackage.Literals.TASK_SET__SUB_TASKS,
				 TaskFactory.eINSTANCE.createSerialTask()));

		newChildDescriptors.add
			(createChildParameter
				(TaskPackage.Literals.TASK_SET__SUB_TASKS,
				 TaskFactory.eINSTANCE.createReadDataSourceTask()));

		newChildDescriptors.add
			(createChildParameter
				(TaskPackage.Literals.TASK_SET__SUB_TASKS,
				 TaskFactory.eINSTANCE.createReportCellCreationTask()));

		newChildDescriptors.add
			(createChildParameter
				(TaskPackage.Literals.TASK_SET__SUB_TASKS,
				 TaskFactory.eINSTANCE.createParallelChoiceTask()));
	}

}
