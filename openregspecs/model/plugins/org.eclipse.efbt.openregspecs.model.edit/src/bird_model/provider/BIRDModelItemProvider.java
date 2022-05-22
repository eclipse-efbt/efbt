/**
 */
package bird_model.provider;


import bird_model.BIRDModel;
import bird_model.Bird_modelPackage;

import java.util.Collection;
import java.util.List;

import org.eclipse.efbt.openregspecs.model.bpmn_lite.Bpmn_liteFactory;
import org.eclipse.efbt.openregspecs.model.core.provider.Smcubes_modelEditPlugin;

import org.eclipse.efbt.openregspecs.model.data_meta_model.Data_meta_modelFactory;

import org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_core_extension.openregspecs_smcubes_core_extensionFactory;

import org.eclipse.efbt.openregspecs.model.requirements_text.Requirements_textFactory;
import org.eclipse.efbt.openregspecs.model.sql_lite.Sql_liteFactory;
import org.eclipse.efbt.openregspecs.model.testing.TestingFactory;
import org.eclipse.emf.common.notify.AdapterFactory;
import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.common.util.ResourceLocator;

import org.eclipse.emf.ecore.EStructuralFeature;

import org.eclipse.emf.edit.provider.IEditingDomainItemProvider;
import org.eclipse.emf.edit.provider.IItemLabelProvider;
import org.eclipse.emf.edit.provider.IItemPropertyDescriptor;
import org.eclipse.emf.edit.provider.IItemPropertySource;
import org.eclipse.emf.edit.provider.IStructuredItemContentProvider;
import org.eclipse.emf.edit.provider.ITreeItemContentProvider;
import org.eclipse.emf.edit.provider.ItemProviderAdapter;
import org.eclipse.emf.edit.provider.ViewerNotification;

/**
 * This is the item provider adapter for a {@link bird_model.BIRDModel} object.
 * <!-- begin-user-doc -->
 * <!-- end-user-doc -->
 * @generated
 */
public class BIRDModelItemProvider 
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
	public BIRDModelItemProvider(AdapterFactory adapterFactory) {
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
			childrenFeatures.add(Bird_modelPackage.Literals.BIRD_MODEL__SM_CUBES_CORE_MODEL);
			childrenFeatures.add(Bird_modelPackage.Literals.BIRD_MODEL__ENTITY_MODULES);
			childrenFeatures.add(Bird_modelPackage.Literals.BIRD_MODEL__SQL_ENTITY_MODULES);
			childrenFeatures.add(Bird_modelPackage.Literals.BIRD_MODEL__SUB_PROCESSES);
			childrenFeatures.add(Bird_modelPackage.Literals.BIRD_MODEL__REQUIREMENTS);
			childrenFeatures.add(Bird_modelPackage.Literals.BIRD_MODEL__TESTS);
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
	 * This returns BIRDModel.gif.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object getImage(Object object) {
		return overlayImage(object, getResourceLocator().getImage("full/obj16/BIRDModel"));
	}

	/**
	 * This returns the label text for the adapted class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public String getText(Object object) {
		return getString("_UI_BIRDModel_type");
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

		switch (notification.getFeatureID(BIRDModel.class)) {
			case Bird_modelPackage.BIRD_MODEL__SM_CUBES_CORE_MODEL:
			case Bird_modelPackage.BIRD_MODEL__ENTITY_MODULES:
			case Bird_modelPackage.BIRD_MODEL__SQL_ENTITY_MODULES:
			case Bird_modelPackage.BIRD_MODEL__SUB_PROCESSES:
			case Bird_modelPackage.BIRD_MODEL__REQUIREMENTS:
			case Bird_modelPackage.BIRD_MODEL__TESTS:
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
				(Bird_modelPackage.Literals.BIRD_MODEL__SM_CUBES_CORE_MODEL,
				 openregspecs_smcubes_core_extensionFactory.eINSTANCE.createSMCubesCoreModel()));

		newChildDescriptors.add
			(createChildParameter
				(Bird_modelPackage.Literals.BIRD_MODEL__ENTITY_MODULES,
				 Data_meta_modelFactory.eINSTANCE.createEntityModule()));

		newChildDescriptors.add
			(createChildParameter
				(Bird_modelPackage.Literals.BIRD_MODEL__SQL_ENTITY_MODULES,
				 Sql_liteFactory.eINSTANCE.createSQLEntityModule()));

		newChildDescriptors.add
			(createChildParameter
				(Bird_modelPackage.Literals.BIRD_MODEL__SUB_PROCESSES,
				 Bpmn_liteFactory.eINSTANCE.createSubProcess()));

		newChildDescriptors.add
			(createChildParameter
				(Bird_modelPackage.Literals.BIRD_MODEL__REQUIREMENTS,
				 Requirements_textFactory.eINSTANCE.createRequirementsModule()));

		newChildDescriptors.add
			(createChildParameter
				(Bird_modelPackage.Literals.BIRD_MODEL__TESTS,
				 TestingFactory.eINSTANCE.createTestModule()));
	}

	/**
	 * Return the resource locator for this item provider's resources.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public ResourceLocator getResourceLocator() {
		return Smcubes_modelEditPlugin.INSTANCE;
	}

}
