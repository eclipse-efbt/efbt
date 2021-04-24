/**
 */
package org.eclipse.efbt.language.dtm.model.transformation.provider;


import java.util.Collection;
import java.util.List;
import org.eclipse.efbt.cocalimo.smcubes.model.program.ProgramFactory;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.efbt_advanced_data_definition.Efbt_advanced_data_definitionFactory;

import org.eclipse.efbt.language.dtm.model.domain_model_mapping.provider.dtmEditPlugin;

import org.eclipse.efbt.language.dtm.model.transformation.TransformationFactory;
import org.eclipse.efbt.language.dtm.model.transformation.TransformationPackage;
import org.eclipse.efbt.language.dtm.model.transformation.dtmProgram;

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
 * This is the item provider adapter for a {@link org.eclipse.efbt.language.dtm.model.transformation.dtmProgram} object.
 * <!-- begin-user-doc -->
 * <!-- end-user-doc -->
 * @generated
 */
public class dtmProgramItemProvider 
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
	public dtmProgramItemProvider(AdapterFactory adapterFactory) {
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
			childrenFeatures.add(TransformationPackage.Literals.DTM_PROGRAM__EXECUTABLE_LOGIC);
			childrenFeatures.add(TransformationPackage.Literals.DTM_PROGRAM__SMCUBES_STATIC_MODEL);
			childrenFeatures.add(TransformationPackage.Literals.DTM_PROGRAM__STRUCT_VARIABLES_MODULE);
			childrenFeatures.add(TransformationPackage.Literals.DTM_PROGRAM__ARRAY_TYPED_VARIABLES_MODULE);
			childrenFeatures.add(TransformationPackage.Literals.DTM_PROGRAM__RELEASE);
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
	 * This returns dtmProgram.gif.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object getImage(Object object) {
		return overlayImage(object, getResourceLocator().getImage("full/obj16/dtmProgram"));
	}

	/**
	 * This returns the label text for the adapted class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public String getText(Object object) {
		return getString("_UI_dtmProgram_type");
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

		switch (notification.getFeatureID(dtmProgram.class)) {
			case TransformationPackage.DTM_PROGRAM__EXECUTABLE_LOGIC:
			case TransformationPackage.DTM_PROGRAM__SMCUBES_STATIC_MODEL:
			case TransformationPackage.DTM_PROGRAM__STRUCT_VARIABLES_MODULE:
			case TransformationPackage.DTM_PROGRAM__ARRAY_TYPED_VARIABLES_MODULE:
			case TransformationPackage.DTM_PROGRAM__RELEASE:
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
				(TransformationPackage.Literals.DTM_PROGRAM__EXECUTABLE_LOGIC,
				 TransformationFactory.eINSTANCE.createdtmExecutableLogic()));

		newChildDescriptors.add
			(createChildParameter
				(TransformationPackage.Literals.DTM_PROGRAM__SMCUBES_STATIC_MODEL,
				 ProgramFactory.eINSTANCE.createSMCubesStaticModel()));

		newChildDescriptors.add
			(createChildParameter
				(TransformationPackage.Literals.DTM_PROGRAM__STRUCT_VARIABLES_MODULE,
				 Efbt_advanced_data_definitionFactory.eINSTANCE.createStructTypedVariablesModule()));

		newChildDescriptors.add
			(createChildParameter
				(TransformationPackage.Literals.DTM_PROGRAM__ARRAY_TYPED_VARIABLES_MODULE,
				 Efbt_advanced_data_definitionFactory.eINSTANCE.createArrayTypedVariableModule()));

		newChildDescriptors.add
			(createChildParameter
				(TransformationPackage.Literals.DTM_PROGRAM__RELEASE,
				 TransformationFactory.eINSTANCE.createRelease()));
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
