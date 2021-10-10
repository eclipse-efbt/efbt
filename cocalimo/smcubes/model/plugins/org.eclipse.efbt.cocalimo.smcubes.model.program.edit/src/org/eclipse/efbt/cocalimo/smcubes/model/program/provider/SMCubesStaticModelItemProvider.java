/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.program.provider;


import java.util.Collection;
import java.util.List;

import org.eclipse.efbt.cocalimo.smcubes.model.program.ProgramPackage;
import org.eclipse.efbt.cocalimo.smcubes.model.program.SMCubesStaticModel;

import org.eclipse.emf.common.notify.AdapterFactory;
import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.edit.provider.ComposeableAdapterFactory;
import org.eclipse.emf.edit.provider.IItemPropertyDescriptor;

/**
 * This is the item provider adapter for a {@link org.eclipse.efbt.cocalimo.smcubes.model.program.SMCubesStaticModel} object.
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

			addSmCubesModelPropertyDescriptor(object);
			addRequirementsPropertyDescriptor(object);
			addBddTestDefinitionsPropertyDescriptor(object);
			addTestsPropertyDescriptor(object);
			addTestTemplatesPropertyDescriptor(object);
			addTestConstriantsPropertyDescriptor(object);
			addTasksPropertyDescriptor(object);
			addRegFunctionalityTestsPropertyDescriptor(object);
			addRegFunctionalityTestDefinitionsPropertyDescriptor(object);
		}
		return itemPropertyDescriptors;
	}

	/**
	 * This adds a property descriptor for the Sm Cubes Model feature.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected void addSmCubesModelPropertyDescriptor(Object object) {
		itemPropertyDescriptors.add
			(createItemPropertyDescriptor
				(((ComposeableAdapterFactory)adapterFactory).getRootAdapterFactory(),
				 getResourceLocator(),
				 getString("_UI_SMCubesStaticModel_smCubesModel_feature"),
				 getString("_UI_PropertyDescriptor_description", "_UI_SMCubesStaticModel_smCubesModel_feature", "_UI_SMCubesStaticModel_type"),
				 ProgramPackage.Literals.SM_CUBES_STATIC_MODEL__SM_CUBES_MODEL,
				 true,
				 false,
				 true,
				 null,
				 null,
				 null));
	}

	/**
	 * This adds a property descriptor for the Requirements feature.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected void addRequirementsPropertyDescriptor(Object object) {
		itemPropertyDescriptors.add
			(createItemPropertyDescriptor
				(((ComposeableAdapterFactory)adapterFactory).getRootAdapterFactory(),
				 getResourceLocator(),
				 getString("_UI_SMCubesStaticModel_requirements_feature"),
				 getString("_UI_PropertyDescriptor_description", "_UI_SMCubesStaticModel_requirements_feature", "_UI_SMCubesStaticModel_type"),
				 ProgramPackage.Literals.SM_CUBES_STATIC_MODEL__REQUIREMENTS,
				 true,
				 false,
				 true,
				 null,
				 null,
				 null));
	}

	/**
	 * This adds a property descriptor for the Bdd Test Definitions feature.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected void addBddTestDefinitionsPropertyDescriptor(Object object) {
		itemPropertyDescriptors.add
			(createItemPropertyDescriptor
				(((ComposeableAdapterFactory)adapterFactory).getRootAdapterFactory(),
				 getResourceLocator(),
				 getString("_UI_SMCubesStaticModel_bddTestDefinitions_feature"),
				 getString("_UI_PropertyDescriptor_description", "_UI_SMCubesStaticModel_bddTestDefinitions_feature", "_UI_SMCubesStaticModel_type"),
				 ProgramPackage.Literals.SM_CUBES_STATIC_MODEL__BDD_TEST_DEFINITIONS,
				 true,
				 false,
				 true,
				 null,
				 null,
				 null));
	}

	/**
	 * This adds a property descriptor for the Tests feature.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected void addTestsPropertyDescriptor(Object object) {
		itemPropertyDescriptors.add
			(createItemPropertyDescriptor
				(((ComposeableAdapterFactory)adapterFactory).getRootAdapterFactory(),
				 getResourceLocator(),
				 getString("_UI_SMCubesStaticModel_tests_feature"),
				 getString("_UI_PropertyDescriptor_description", "_UI_SMCubesStaticModel_tests_feature", "_UI_SMCubesStaticModel_type"),
				 ProgramPackage.Literals.SM_CUBES_STATIC_MODEL__TESTS,
				 true,
				 false,
				 true,
				 null,
				 null,
				 null));
	}

	/**
	 * This adds a property descriptor for the Test Templates feature.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected void addTestTemplatesPropertyDescriptor(Object object) {
		itemPropertyDescriptors.add
			(createItemPropertyDescriptor
				(((ComposeableAdapterFactory)adapterFactory).getRootAdapterFactory(),
				 getResourceLocator(),
				 getString("_UI_SMCubesStaticModel_testTemplates_feature"),
				 getString("_UI_PropertyDescriptor_description", "_UI_SMCubesStaticModel_testTemplates_feature", "_UI_SMCubesStaticModel_type"),
				 ProgramPackage.Literals.SM_CUBES_STATIC_MODEL__TEST_TEMPLATES,
				 true,
				 false,
				 true,
				 null,
				 null,
				 null));
	}

	/**
	 * This adds a property descriptor for the Test Constriants feature.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected void addTestConstriantsPropertyDescriptor(Object object) {
		itemPropertyDescriptors.add
			(createItemPropertyDescriptor
				(((ComposeableAdapterFactory)adapterFactory).getRootAdapterFactory(),
				 getResourceLocator(),
				 getString("_UI_SMCubesStaticModel_testConstriants_feature"),
				 getString("_UI_PropertyDescriptor_description", "_UI_SMCubesStaticModel_testConstriants_feature", "_UI_SMCubesStaticModel_type"),
				 ProgramPackage.Literals.SM_CUBES_STATIC_MODEL__TEST_CONSTRIANTS,
				 true,
				 false,
				 true,
				 null,
				 null,
				 null));
	}

	/**
	 * This adds a property descriptor for the Functionality Modules feature.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected void addTasksPropertyDescriptor(Object object) {
		itemPropertyDescriptors.add
			(createItemPropertyDescriptor
				(((ComposeableAdapterFactory)adapterFactory).getRootAdapterFactory(),
				 getResourceLocator(),
				 getString("_UI_SMCubesStaticModel_functionalityModules_feature"),
				 getString("_UI_PropertyDescriptor_description", "_UI_SMCubesStaticModel_functionalityModules_feature", "_UI_SMCubesStaticModel_type"),
				 ProgramPackage.Literals.SM_CUBES_STATIC_MODEL__TASKS,
				 true,
				 false,
				 true,
				 null,
				 null,
				 null));
	}

	/**
	 * This adds a property descriptor for the Reg Functionality Tests feature.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected void addRegFunctionalityTestsPropertyDescriptor(Object object) {
		itemPropertyDescriptors.add
			(createItemPropertyDescriptor
				(((ComposeableAdapterFactory)adapterFactory).getRootAdapterFactory(),
				 getResourceLocator(),
				 getString("_UI_SMCubesStaticModel_regFunctionalityTests_feature"),
				 getString("_UI_PropertyDescriptor_description", "_UI_SMCubesStaticModel_regFunctionalityTests_feature", "_UI_SMCubesStaticModel_type"),
				 ProgramPackage.Literals.SM_CUBES_STATIC_MODEL__REG_FUNCTIONALITY_TESTS,
				 true,
				 false,
				 true,
				 null,
				 null,
				 null));
	}

	/**
	 * This adds a property descriptor for the Reg Functionality Test Definitions feature.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected void addRegFunctionalityTestDefinitionsPropertyDescriptor(Object object) {
		itemPropertyDescriptors.add
			(createItemPropertyDescriptor
				(((ComposeableAdapterFactory)adapterFactory).getRootAdapterFactory(),
				 getResourceLocator(),
				 getString("_UI_SMCubesStaticModel_regFunctionalityTestDefinitions_feature"),
				 getString("_UI_PropertyDescriptor_description", "_UI_SMCubesStaticModel_regFunctionalityTestDefinitions_feature", "_UI_SMCubesStaticModel_type"),
				 ProgramPackage.Literals.SM_CUBES_STATIC_MODEL__REG_FUNCTIONALITY_TEST_DEFINITIONS,
				 true,
				 false,
				 true,
				 null,
				 null,
				 null));
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
