/**
 */
package org.eclipse.efbt.language.dtm.model.transformation.provider;


import java.util.Collection;
import java.util.List;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.CubesFactory;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.FunctionsFactory;

import org.eclipse.efbt.language.dtm.model.domain_model_mapping.provider.dtmEditPlugin;

import org.eclipse.efbt.language.dtm.model.dtm_report_cell_views.dtm_report_cell_viewsFactory;

import org.eclipse.efbt.language.dtm.model.dtm_views.dtm_viewsFactory;

import org.eclipse.efbt.language.dtm.model.transformation.TransformationFactory;
import org.eclipse.efbt.language.dtm.model.transformation.TransformationPackage;
import org.eclipse.efbt.language.dtm.model.transformation.dtmExecutableLogic;

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
 * This is the item provider adapter for a {@link org.eclipse.efbt.language.dtm.model.transformation.dtmExecutableLogic} object.
 * <!-- begin-user-doc -->
 * <!-- end-user-doc -->
 * @generated
 */
public class dtmExecutableLogicItemProvider 
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
	public dtmExecutableLogicItemProvider(AdapterFactory adapterFactory) {
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

			addNamePropertyDescriptor(object);
		}
		return itemPropertyDescriptors;
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
				 getString("_UI_dtmExecutableLogic_name_feature"),
				 getString("_UI_PropertyDescriptor_description", "_UI_dtmExecutableLogic_name_feature", "_UI_dtmExecutableLogic_type"),
				 TransformationPackage.Literals.DTM_EXECUTABLE_LOGIC__NAME,
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
			childrenFeatures.add(TransformationPackage.Literals.DTM_EXECUTABLE_LOGIC__CUBES_MODULE);
			childrenFeatures.add(TransformationPackage.Literals.DTM_EXECUTABLE_LOGIC__FUNCTION_SPEC_MODULE);
			childrenFeatures.add(TransformationPackage.Literals.DTM_EXECUTABLE_LOGIC__CUBES_HIERARCHY_MODULE);
			childrenFeatures.add(TransformationPackage.Literals.DTM_EXECUTABLE_LOGIC__CUBE_SCHEMA_MODULES);
			childrenFeatures.add(TransformationPackage.Literals.DTM_EXECUTABLE_LOGIC__DATASET_TRANSFORMATION_MODULES);
			childrenFeatures.add(TransformationPackage.Literals.DTM_EXECUTABLE_LOGIC__REPORT_CELL_VIEW_MODULES);
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
	 * This returns dtmExecutableLogic.gif.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object getImage(Object object) {
		return overlayImage(object, getResourceLocator().getImage("full/obj16/dtmExecutableLogic"));
	}

	/**
	 * This returns the label text for the adapted class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public String getText(Object object) {
		String label = ((dtmExecutableLogic)object).getName();
		return label == null || label.length() == 0 ?
			getString("_UI_dtmExecutableLogic_type") :
			getString("_UI_dtmExecutableLogic_type") + " " + label;
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

		switch (notification.getFeatureID(dtmExecutableLogic.class)) {
			case TransformationPackage.DTM_EXECUTABLE_LOGIC__NAME:
				fireNotifyChanged(new ViewerNotification(notification, notification.getNotifier(), false, true));
				return;
			case TransformationPackage.DTM_EXECUTABLE_LOGIC__CUBES_MODULE:
			case TransformationPackage.DTM_EXECUTABLE_LOGIC__FUNCTION_SPEC_MODULE:
			case TransformationPackage.DTM_EXECUTABLE_LOGIC__CUBES_HIERARCHY_MODULE:
			case TransformationPackage.DTM_EXECUTABLE_LOGIC__CUBE_SCHEMA_MODULES:
			case TransformationPackage.DTM_EXECUTABLE_LOGIC__DATASET_TRANSFORMATION_MODULES:
			case TransformationPackage.DTM_EXECUTABLE_LOGIC__REPORT_CELL_VIEW_MODULES:
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
				(TransformationPackage.Literals.DTM_EXECUTABLE_LOGIC__CUBES_MODULE,
				 CubesFactory.eINSTANCE.createFreeBirdToolsCubeModule()));

		newChildDescriptors.add
			(createChildParameter
				(TransformationPackage.Literals.DTM_EXECUTABLE_LOGIC__FUNCTION_SPEC_MODULE,
				 FunctionsFactory.eINSTANCE.createFunctionSpecModule()));

		newChildDescriptors.add
			(createChildParameter
				(TransformationPackage.Literals.DTM_EXECUTABLE_LOGIC__CUBES_HIERARCHY_MODULE,
				 CubesFactory.eINSTANCE.createFreeBIRDToolsCubeHierarchyRelationshipModule()));

		newChildDescriptors.add
			(createChildParameter
				(TransformationPackage.Literals.DTM_EXECUTABLE_LOGIC__CUBE_SCHEMA_MODULES,
				 TransformationFactory.eINSTANCE.createVersionedCubeSchemaModule()));

		newChildDescriptors.add
			(createChildParameter
				(TransformationPackage.Literals.DTM_EXECUTABLE_LOGIC__DATASET_TRANSFORMATION_MODULES,
				 dtm_viewsFactory.eINSTANCE.createVersionedDTMViewsModule()));

		newChildDescriptors.add
			(createChildParameter
				(TransformationPackage.Literals.DTM_EXECUTABLE_LOGIC__REPORT_CELL_VIEW_MODULES,
				 dtm_report_cell_viewsFactory.eINSTANCE.createReportCellViewModule()));
	}

	/**
	 * Return the resource locator for this item provider's resources.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public ResourceLocator getResourceLocator() {
		return dtmEditPlugin.INSTANCE;
	}

}
