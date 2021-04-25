/**
 */
package org.eclipse.efbt.language.dtm.model.transformation.impl;

import java.util.Collection;
import org.eclipse.efbt.cocalimo.smcubes.model.program.SMCubesStaticModel;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.efbt_advanced_data_definition.ArrayTypedVariableModule;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.efbt_advanced_data_definition.StructTypedVariablesModule;

import org.eclipse.efbt.language.dtm.model.transformation.Release;
import org.eclipse.efbt.language.dtm.model.transformation.TransformationPackage;
import org.eclipse.efbt.language.dtm.model.transformation.dtmExecutableLogic;
import org.eclipse.efbt.language.dtm.model.transformation.dtmProgram;

import org.eclipse.emf.common.notify.Notification;
import org.eclipse.emf.common.notify.NotificationChain;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;
import org.eclipse.emf.ecore.impl.MinimalEObjectImpl;

import org.eclipse.emf.ecore.util.EObjectContainmentEList;
import org.eclipse.emf.ecore.util.InternalEList;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>dtm Program</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.language.dtm.model.transformation.impl.dtmProgramImpl#getExecutableLogic <em>Executable Logic</em>}</li>
 *   <li>{@link org.eclipse.efbt.language.dtm.model.transformation.impl.dtmProgramImpl#getSmcubesStaticModel <em>Smcubes Static Model</em>}</li>
 *   <li>{@link org.eclipse.efbt.language.dtm.model.transformation.impl.dtmProgramImpl#getStructVariablesModule <em>Struct Variables Module</em>}</li>
 *   <li>{@link org.eclipse.efbt.language.dtm.model.transformation.impl.dtmProgramImpl#getArrayTypedVariablesModule <em>Array Typed Variables Module</em>}</li>
 *   <li>{@link org.eclipse.efbt.language.dtm.model.transformation.impl.dtmProgramImpl#getRelease <em>Release</em>}</li>
 * </ul>
 *
 * @generated
 */
public class dtmProgramImpl extends MinimalEObjectImpl.Container implements dtmProgram {
	/**
	 * The cached value of the '{@link #getExecutableLogic() <em>Executable Logic</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getExecutableLogic()
	 * @generated
	 * @ordered
	 */
	protected EList<dtmExecutableLogic> executableLogic;

	/**
	 * The cached value of the '{@link #getSmcubesStaticModel() <em>Smcubes Static Model</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getSmcubesStaticModel()
	 * @generated
	 * @ordered
	 */
	protected SMCubesStaticModel smcubesStaticModel;

	/**
	 * The cached value of the '{@link #getStructVariablesModule() <em>Struct Variables Module</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getStructVariablesModule()
	 * @generated
	 * @ordered
	 */
	protected StructTypedVariablesModule structVariablesModule;

	/**
	 * The cached value of the '{@link #getArrayTypedVariablesModule() <em>Array Typed Variables Module</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getArrayTypedVariablesModule()
	 * @generated
	 * @ordered
	 */
	protected ArrayTypedVariableModule arrayTypedVariablesModule;

	/**
	 * The cached value of the '{@link #getRelease() <em>Release</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getRelease()
	 * @generated
	 * @ordered
	 */
	protected Release release;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected dtmProgramImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return TransformationPackage.Literals.DTM_PROGRAM;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EList<dtmExecutableLogic> getExecutableLogic() {
		if (executableLogic == null) {
			executableLogic = new EObjectContainmentEList<dtmExecutableLogic>(dtmExecutableLogic.class, this, TransformationPackage.DTM_PROGRAM__EXECUTABLE_LOGIC);
		}
		return executableLogic;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public SMCubesStaticModel getSmcubesStaticModel() {
		return smcubesStaticModel;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public NotificationChain basicSetSmcubesStaticModel(SMCubesStaticModel newSmcubesStaticModel, NotificationChain msgs) {
		SMCubesStaticModel oldSmcubesStaticModel = smcubesStaticModel;
		smcubesStaticModel = newSmcubesStaticModel;
		if (eNotificationRequired()) {
			ENotificationImpl notification = new ENotificationImpl(this, Notification.SET, TransformationPackage.DTM_PROGRAM__SMCUBES_STATIC_MODEL, oldSmcubesStaticModel, newSmcubesStaticModel);
			if (msgs == null) msgs = notification; else msgs.add(notification);
		}
		return msgs;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setSmcubesStaticModel(SMCubesStaticModel newSmcubesStaticModel) {
		if (newSmcubesStaticModel != smcubesStaticModel) {
			NotificationChain msgs = null;
			if (smcubesStaticModel != null)
				msgs = ((InternalEObject)smcubesStaticModel).eInverseRemove(this, EOPPOSITE_FEATURE_BASE - TransformationPackage.DTM_PROGRAM__SMCUBES_STATIC_MODEL, null, msgs);
			if (newSmcubesStaticModel != null)
				msgs = ((InternalEObject)newSmcubesStaticModel).eInverseAdd(this, EOPPOSITE_FEATURE_BASE - TransformationPackage.DTM_PROGRAM__SMCUBES_STATIC_MODEL, null, msgs);
			msgs = basicSetSmcubesStaticModel(newSmcubesStaticModel, msgs);
			if (msgs != null) msgs.dispatch();
		}
		else if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, TransformationPackage.DTM_PROGRAM__SMCUBES_STATIC_MODEL, newSmcubesStaticModel, newSmcubesStaticModel));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public StructTypedVariablesModule getStructVariablesModule() {
		return structVariablesModule;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public NotificationChain basicSetStructVariablesModule(StructTypedVariablesModule newStructVariablesModule, NotificationChain msgs) {
		StructTypedVariablesModule oldStructVariablesModule = structVariablesModule;
		structVariablesModule = newStructVariablesModule;
		if (eNotificationRequired()) {
			ENotificationImpl notification = new ENotificationImpl(this, Notification.SET, TransformationPackage.DTM_PROGRAM__STRUCT_VARIABLES_MODULE, oldStructVariablesModule, newStructVariablesModule);
			if (msgs == null) msgs = notification; else msgs.add(notification);
		}
		return msgs;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setStructVariablesModule(StructTypedVariablesModule newStructVariablesModule) {
		if (newStructVariablesModule != structVariablesModule) {
			NotificationChain msgs = null;
			if (structVariablesModule != null)
				msgs = ((InternalEObject)structVariablesModule).eInverseRemove(this, EOPPOSITE_FEATURE_BASE - TransformationPackage.DTM_PROGRAM__STRUCT_VARIABLES_MODULE, null, msgs);
			if (newStructVariablesModule != null)
				msgs = ((InternalEObject)newStructVariablesModule).eInverseAdd(this, EOPPOSITE_FEATURE_BASE - TransformationPackage.DTM_PROGRAM__STRUCT_VARIABLES_MODULE, null, msgs);
			msgs = basicSetStructVariablesModule(newStructVariablesModule, msgs);
			if (msgs != null) msgs.dispatch();
		}
		else if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, TransformationPackage.DTM_PROGRAM__STRUCT_VARIABLES_MODULE, newStructVariablesModule, newStructVariablesModule));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public ArrayTypedVariableModule getArrayTypedVariablesModule() {
		return arrayTypedVariablesModule;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public NotificationChain basicSetArrayTypedVariablesModule(ArrayTypedVariableModule newArrayTypedVariablesModule, NotificationChain msgs) {
		ArrayTypedVariableModule oldArrayTypedVariablesModule = arrayTypedVariablesModule;
		arrayTypedVariablesModule = newArrayTypedVariablesModule;
		if (eNotificationRequired()) {
			ENotificationImpl notification = new ENotificationImpl(this, Notification.SET, TransformationPackage.DTM_PROGRAM__ARRAY_TYPED_VARIABLES_MODULE, oldArrayTypedVariablesModule, newArrayTypedVariablesModule);
			if (msgs == null) msgs = notification; else msgs.add(notification);
		}
		return msgs;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setArrayTypedVariablesModule(ArrayTypedVariableModule newArrayTypedVariablesModule) {
		if (newArrayTypedVariablesModule != arrayTypedVariablesModule) {
			NotificationChain msgs = null;
			if (arrayTypedVariablesModule != null)
				msgs = ((InternalEObject)arrayTypedVariablesModule).eInverseRemove(this, EOPPOSITE_FEATURE_BASE - TransformationPackage.DTM_PROGRAM__ARRAY_TYPED_VARIABLES_MODULE, null, msgs);
			if (newArrayTypedVariablesModule != null)
				msgs = ((InternalEObject)newArrayTypedVariablesModule).eInverseAdd(this, EOPPOSITE_FEATURE_BASE - TransformationPackage.DTM_PROGRAM__ARRAY_TYPED_VARIABLES_MODULE, null, msgs);
			msgs = basicSetArrayTypedVariablesModule(newArrayTypedVariablesModule, msgs);
			if (msgs != null) msgs.dispatch();
		}
		else if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, TransformationPackage.DTM_PROGRAM__ARRAY_TYPED_VARIABLES_MODULE, newArrayTypedVariablesModule, newArrayTypedVariablesModule));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Release getRelease() {
		return release;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public NotificationChain basicSetRelease(Release newRelease, NotificationChain msgs) {
		Release oldRelease = release;
		release = newRelease;
		if (eNotificationRequired()) {
			ENotificationImpl notification = new ENotificationImpl(this, Notification.SET, TransformationPackage.DTM_PROGRAM__RELEASE, oldRelease, newRelease);
			if (msgs == null) msgs = notification; else msgs.add(notification);
		}
		return msgs;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setRelease(Release newRelease) {
		if (newRelease != release) {
			NotificationChain msgs = null;
			if (release != null)
				msgs = ((InternalEObject)release).eInverseRemove(this, EOPPOSITE_FEATURE_BASE - TransformationPackage.DTM_PROGRAM__RELEASE, null, msgs);
			if (newRelease != null)
				msgs = ((InternalEObject)newRelease).eInverseAdd(this, EOPPOSITE_FEATURE_BASE - TransformationPackage.DTM_PROGRAM__RELEASE, null, msgs);
			msgs = basicSetRelease(newRelease, msgs);
			if (msgs != null) msgs.dispatch();
		}
		else if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, TransformationPackage.DTM_PROGRAM__RELEASE, newRelease, newRelease));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public NotificationChain eInverseRemove(InternalEObject otherEnd, int featureID, NotificationChain msgs) {
		switch (featureID) {
			case TransformationPackage.DTM_PROGRAM__EXECUTABLE_LOGIC:
				return ((InternalEList<?>)getExecutableLogic()).basicRemove(otherEnd, msgs);
			case TransformationPackage.DTM_PROGRAM__SMCUBES_STATIC_MODEL:
				return basicSetSmcubesStaticModel(null, msgs);
			case TransformationPackage.DTM_PROGRAM__STRUCT_VARIABLES_MODULE:
				return basicSetStructVariablesModule(null, msgs);
			case TransformationPackage.DTM_PROGRAM__ARRAY_TYPED_VARIABLES_MODULE:
				return basicSetArrayTypedVariablesModule(null, msgs);
			case TransformationPackage.DTM_PROGRAM__RELEASE:
				return basicSetRelease(null, msgs);
		}
		return super.eInverseRemove(otherEnd, featureID, msgs);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object eGet(int featureID, boolean resolve, boolean coreType) {
		switch (featureID) {
			case TransformationPackage.DTM_PROGRAM__EXECUTABLE_LOGIC:
				return getExecutableLogic();
			case TransformationPackage.DTM_PROGRAM__SMCUBES_STATIC_MODEL:
				return getSmcubesStaticModel();
			case TransformationPackage.DTM_PROGRAM__STRUCT_VARIABLES_MODULE:
				return getStructVariablesModule();
			case TransformationPackage.DTM_PROGRAM__ARRAY_TYPED_VARIABLES_MODULE:
				return getArrayTypedVariablesModule();
			case TransformationPackage.DTM_PROGRAM__RELEASE:
				return getRelease();
		}
		return super.eGet(featureID, resolve, coreType);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@SuppressWarnings("unchecked")
	@Override
	public void eSet(int featureID, Object newValue) {
		switch (featureID) {
			case TransformationPackage.DTM_PROGRAM__EXECUTABLE_LOGIC:
				getExecutableLogic().clear();
				getExecutableLogic().addAll((Collection<? extends dtmExecutableLogic>)newValue);
				return;
			case TransformationPackage.DTM_PROGRAM__SMCUBES_STATIC_MODEL:
				setSmcubesStaticModel((SMCubesStaticModel)newValue);
				return;
			case TransformationPackage.DTM_PROGRAM__STRUCT_VARIABLES_MODULE:
				setStructVariablesModule((StructTypedVariablesModule)newValue);
				return;
			case TransformationPackage.DTM_PROGRAM__ARRAY_TYPED_VARIABLES_MODULE:
				setArrayTypedVariablesModule((ArrayTypedVariableModule)newValue);
				return;
			case TransformationPackage.DTM_PROGRAM__RELEASE:
				setRelease((Release)newValue);
				return;
		}
		super.eSet(featureID, newValue);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void eUnset(int featureID) {
		switch (featureID) {
			case TransformationPackage.DTM_PROGRAM__EXECUTABLE_LOGIC:
				getExecutableLogic().clear();
				return;
			case TransformationPackage.DTM_PROGRAM__SMCUBES_STATIC_MODEL:
				setSmcubesStaticModel((SMCubesStaticModel)null);
				return;
			case TransformationPackage.DTM_PROGRAM__STRUCT_VARIABLES_MODULE:
				setStructVariablesModule((StructTypedVariablesModule)null);
				return;
			case TransformationPackage.DTM_PROGRAM__ARRAY_TYPED_VARIABLES_MODULE:
				setArrayTypedVariablesModule((ArrayTypedVariableModule)null);
				return;
			case TransformationPackage.DTM_PROGRAM__RELEASE:
				setRelease((Release)null);
				return;
		}
		super.eUnset(featureID);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public boolean eIsSet(int featureID) {
		switch (featureID) {
			case TransformationPackage.DTM_PROGRAM__EXECUTABLE_LOGIC:
				return executableLogic != null && !executableLogic.isEmpty();
			case TransformationPackage.DTM_PROGRAM__SMCUBES_STATIC_MODEL:
				return smcubesStaticModel != null;
			case TransformationPackage.DTM_PROGRAM__STRUCT_VARIABLES_MODULE:
				return structVariablesModule != null;
			case TransformationPackage.DTM_PROGRAM__ARRAY_TYPED_VARIABLES_MODULE:
				return arrayTypedVariablesModule != null;
			case TransformationPackage.DTM_PROGRAM__RELEASE:
				return release != null;
		}
		return super.eIsSet(featureID);
	}

} //dtmProgramImpl
