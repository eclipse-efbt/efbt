/**
 */
package org.eclipse.efbt.cocalimo.core.model.functionality_module.provider;


import java.util.Collection;
import java.util.List;

import org.eclipse.efbt.cocalimo.core.model.functionality_module.FunctionalityModuleModule;
import org.eclipse.efbt.cocalimo.core.model.functionality_module.Functionality_moduleFactory;
import org.eclipse.efbt.cocalimo.core.model.functionality_module.Functionality_modulePackage;

import org.eclipse.efbt.cocalimo.core.model.module_management.provider.ModuleItemProvider;

import org.eclipse.efbt.cocalimo.core.model.scenarios.ScenariosFactory;

import org.eclipse.emf.common.notify.AdapterFactory;
import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.common.util.ResourceLocator;

import org.eclipse.emf.ecore.EStructuralFeature;

import org.eclipse.emf.edit.provider.IItemPropertyDescriptor;
import org.eclipse.emf.edit.provider.ViewerNotification;

/**
 * This is the item provider adapter for a {@link org.eclipse.efbt.cocalimo.core.model.functionality_module.FunctionalityModuleModule} object.
 * <!-- begin-user-doc -->
 * <!-- end-user-doc -->
 * @generated
 */
public class FunctionalityModuleModuleItemProvider extends ModuleItemProvider {
	/**
	 * This constructs an instance from a factory and a notifier.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public FunctionalityModuleModuleItemProvider(AdapterFactory adapterFactory) {
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
			childrenFeatures.add(Functionality_modulePackage.Literals.FUNCTIONALITY_MODULE_MODULE__FUNCTIONALITY_MODULES);
			childrenFeatures.add(Functionality_modulePackage.Literals.FUNCTIONALITY_MODULE_MODULE__FUNCTIONALITY_MODULE_TAGS);
			childrenFeatures.add(Functionality_modulePackage.Literals.FUNCTIONALITY_MODULE_MODULE__SCEANRIO_TAGS);
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
	 * This returns FunctionalityModuleModule.gif.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object getImage(Object object) {
		return overlayImage(object, getResourceLocator().getImage("full/obj16/FunctionalityModuleModule"));
	}

	/**
	 * This returns the label text for the adapted class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public String getText(Object object) {
		String label = ((FunctionalityModuleModule)object).getName();
		return label == null || label.length() == 0 ?
			getString("_UI_FunctionalityModuleModule_type") :
			getString("_UI_FunctionalityModuleModule_type") + " " + label;
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

		switch (notification.getFeatureID(FunctionalityModuleModule.class)) {
			case Functionality_modulePackage.FUNCTIONALITY_MODULE_MODULE__FUNCTIONALITY_MODULES:
			case Functionality_modulePackage.FUNCTIONALITY_MODULE_MODULE__FUNCTIONALITY_MODULE_TAGS:
			case Functionality_modulePackage.FUNCTIONALITY_MODULE_MODULE__SCEANRIO_TAGS:
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
				(Functionality_modulePackage.Literals.FUNCTIONALITY_MODULE_MODULE__FUNCTIONALITY_MODULES,
				 Functionality_moduleFactory.eINSTANCE.createLeafFunctionalityModule()));

		newChildDescriptors.add
			(createChildParameter
				(Functionality_modulePackage.Literals.FUNCTIONALITY_MODULE_MODULE__FUNCTIONALITY_MODULES,
				 Functionality_moduleFactory.eINSTANCE.createDataProcessingFunctionalityModule()));

		newChildDescriptors.add
			(createChildParameter
				(Functionality_modulePackage.Literals.FUNCTIONALITY_MODULE_MODULE__FUNCTIONALITY_MODULES,
				 Functionality_moduleFactory.eINSTANCE.createFunctionalityModuleSet()));

		newChildDescriptors.add
			(createChildParameter
				(Functionality_modulePackage.Literals.FUNCTIONALITY_MODULE_MODULE__FUNCTIONALITY_MODULES,
				 Functionality_moduleFactory.eINSTANCE.createParallelFunctionalityModule()));

		newChildDescriptors.add
			(createChildParameter
				(Functionality_modulePackage.Literals.FUNCTIONALITY_MODULE_MODULE__FUNCTIONALITY_MODULES,
				 Functionality_moduleFactory.eINSTANCE.createScenarioSetFunctionalityModule()));

		newChildDescriptors.add
			(createChildParameter
				(Functionality_modulePackage.Literals.FUNCTIONALITY_MODULE_MODULE__FUNCTIONALITY_MODULES,
				 Functionality_moduleFactory.eINSTANCE.createSerialFunctionalityModule()));

		newChildDescriptors.add
			(createChildParameter
				(Functionality_modulePackage.Literals.FUNCTIONALITY_MODULE_MODULE__FUNCTIONALITY_MODULES,
				 Functionality_moduleFactory.eINSTANCE.createReadDataSourceFunctionalityModule()));

		newChildDescriptors.add
			(createChildParameter
				(Functionality_modulePackage.Literals.FUNCTIONALITY_MODULE_MODULE__FUNCTIONALITY_MODULES,
				 Functionality_moduleFactory.eINSTANCE.createReportCellCreationFunctionalityModule()));

		newChildDescriptors.add
			(createChildParameter
				(Functionality_modulePackage.Literals.FUNCTIONALITY_MODULE_MODULE__FUNCTIONALITY_MODULES,
				 Functionality_moduleFactory.eINSTANCE.createValidationFunctionalityModule()));

		newChildDescriptors.add
			(createChildParameter
				(Functionality_modulePackage.Literals.FUNCTIONALITY_MODULE_MODULE__FUNCTIONALITY_MODULES,
				 Functionality_moduleFactory.eINSTANCE.createParallelChoiceFunctionalityModule()));

		newChildDescriptors.add
			(createChildParameter
				(Functionality_modulePackage.Literals.FUNCTIONALITY_MODULE_MODULE__FUNCTIONALITY_MODULE_TAGS,
				 Functionality_moduleFactory.eINSTANCE.createFunctionalityModuleTag()));

		newChildDescriptors.add
			(createChildParameter
				(Functionality_modulePackage.Literals.FUNCTIONALITY_MODULE_MODULE__SCEANRIO_TAGS,
				 ScenariosFactory.eINSTANCE.createScenarioTag()));
	}

	/**
	 * Return the resource locator for this item provider's resources.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public ResourceLocator getResourceLocator() {
		return Functionality_moduleEditPlugin.INSTANCE;
	}

}
