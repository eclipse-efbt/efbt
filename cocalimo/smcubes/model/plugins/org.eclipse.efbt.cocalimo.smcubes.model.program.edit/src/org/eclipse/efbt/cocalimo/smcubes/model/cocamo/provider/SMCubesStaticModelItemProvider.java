/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.cocamo.provider;


import java.util.Collection;
import java.util.List;

import org.eclipse.efbt.cocalimo.core.model.functionality_module.Functionality_moduleFactory;

import org.eclipse.efbt.cocalimo.core.model.requirements_text.Requirements_textFactory;

import org.eclipse.efbt.cocalimo.core.model.test_definition.Test_definitionFactory;

import org.eclipse.efbt.cocalimo.smcubes.model.cocamo.CocamoFactory;
import org.eclipse.efbt.cocalimo.smcubes.model.cocamo.CocamoPackage;
import org.eclipse.efbt.cocalimo.smcubes.model.cocamo.SMCubesStaticModel;

import org.eclipse.efbt.cocalimo.smcubes.model.smcubes_model.Smcubes_modelFactory;

import org.eclipse.emf.common.notify.AdapterFactory;
import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.ecore.EStructuralFeature;

import org.eclipse.emf.edit.provider.IItemPropertyDescriptor;
import org.eclipse.emf.edit.provider.ViewerNotification;

/**
 * This is the item provider adapter for a {@link org.eclipse.efbt.cocalimo.smcubes.model.cocamo.SMCubesStaticModel} object.
 * <!-- begin-user-doc -->
 * <!-- end-user-doc -->
 * @generated
 */
public class SMCubesStaticModelItemProvider extends StaticModelItemProvider {
	/**
	 * This constructs an instance from a factory and a notifier.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public SMCubesStaticModelItemProvider(AdapterFactory adapterFactory) {
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
			childrenFeatures.add(CocamoPackage.Literals.SM_CUBES_STATIC_MODEL__SM_CUBES_MODEL);
			childrenFeatures.add(CocamoPackage.Literals.SM_CUBES_STATIC_MODEL__REQUIREMENTS);
			childrenFeatures.add(CocamoPackage.Literals.SM_CUBES_STATIC_MODEL__BDD_TEST_DEFINITIONS);
			childrenFeatures.add(CocamoPackage.Literals.SM_CUBES_STATIC_MODEL__TESTS);
			childrenFeatures.add(CocamoPackage.Literals.SM_CUBES_STATIC_MODEL__TEST_TEMPLATES);
			childrenFeatures.add(CocamoPackage.Literals.SM_CUBES_STATIC_MODEL__TEST_CONSTRIANTS);
			childrenFeatures.add(CocamoPackage.Literals.SM_CUBES_STATIC_MODEL__FUNCTIONALITY_MODULES);
			childrenFeatures.add(CocamoPackage.Literals.SM_CUBES_STATIC_MODEL__REG_FUNCTIONALITY_TESTS);
			childrenFeatures.add(CocamoPackage.Literals.SM_CUBES_STATIC_MODEL__REG_FUNCTIONALITY_TEST_DEFINITIONS);
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
	 * This returns SMCubesStaticModel.gif.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object getImage(Object object) {
		return overlayImage(object, getResourceLocator().getImage("full/obj16/SMCubesStaticModel"));
	}

	/**
	 * This returns the label text for the adapted class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public String getText(Object object) {
		String label = ((SMCubesStaticModel)object).getName();
		return label == null || label.length() == 0 ?
			getString("_UI_SMCubesStaticModel_type") :
			getString("_UI_SMCubesStaticModel_type") + " " + label;
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

		switch (notification.getFeatureID(SMCubesStaticModel.class)) {
			case CocamoPackage.SM_CUBES_STATIC_MODEL__SM_CUBES_MODEL:
			case CocamoPackage.SM_CUBES_STATIC_MODEL__REQUIREMENTS:
			case CocamoPackage.SM_CUBES_STATIC_MODEL__BDD_TEST_DEFINITIONS:
			case CocamoPackage.SM_CUBES_STATIC_MODEL__TESTS:
			case CocamoPackage.SM_CUBES_STATIC_MODEL__TEST_TEMPLATES:
			case CocamoPackage.SM_CUBES_STATIC_MODEL__TEST_CONSTRIANTS:
			case CocamoPackage.SM_CUBES_STATIC_MODEL__FUNCTIONALITY_MODULES:
			case CocamoPackage.SM_CUBES_STATIC_MODEL__REG_FUNCTIONALITY_TESTS:
			case CocamoPackage.SM_CUBES_STATIC_MODEL__REG_FUNCTIONALITY_TEST_DEFINITIONS:
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
				(CocamoPackage.Literals.SM_CUBES_STATIC_MODEL__SM_CUBES_MODEL,
				 Smcubes_modelFactory.eINSTANCE.createSmcubesModel()));

		newChildDescriptors.add
			(createChildParameter
				(CocamoPackage.Literals.SM_CUBES_STATIC_MODEL__REQUIREMENTS,
				 Requirements_textFactory.eINSTANCE.createRequirementsModule()));

		newChildDescriptors.add
			(createChildParameter
				(CocamoPackage.Literals.SM_CUBES_STATIC_MODEL__BDD_TEST_DEFINITIONS,
				 Test_definitionFactory.eINSTANCE.createBDDTestDefinitionModule()));

		newChildDescriptors.add
			(createChildParameter
				(CocamoPackage.Literals.SM_CUBES_STATIC_MODEL__TESTS,
				 CocamoFactory.eINSTANCE.createSMCubesBDDTestModule()));

		newChildDescriptors.add
			(createChildParameter
				(CocamoPackage.Literals.SM_CUBES_STATIC_MODEL__TEST_TEMPLATES,
				 Test_definitionFactory.eINSTANCE.createBDDTestTemplateModule()));

		newChildDescriptors.add
			(createChildParameter
				(CocamoPackage.Literals.SM_CUBES_STATIC_MODEL__TEST_CONSTRIANTS,
				 Test_definitionFactory.eINSTANCE.createBDDTestContraints()));

		newChildDescriptors.add
			(createChildParameter
				(CocamoPackage.Literals.SM_CUBES_STATIC_MODEL__FUNCTIONALITY_MODULES,
				 Functionality_moduleFactory.eINSTANCE.createFunctionalityModuleModule()));

		newChildDescriptors.add
			(createChildParameter
				(CocamoPackage.Literals.SM_CUBES_STATIC_MODEL__REG_FUNCTIONALITY_TESTS,
				 CocamoFactory.eINSTANCE.createSMCubesRegFunctionalityTestModule()));

		newChildDescriptors.add
			(createChildParameter
				(CocamoPackage.Literals.SM_CUBES_STATIC_MODEL__REG_FUNCTIONALITY_TEST_DEFINITIONS,
				 Test_definitionFactory.eINSTANCE.createRegFunctionalityTestDefinitionModule()));
	}

}
