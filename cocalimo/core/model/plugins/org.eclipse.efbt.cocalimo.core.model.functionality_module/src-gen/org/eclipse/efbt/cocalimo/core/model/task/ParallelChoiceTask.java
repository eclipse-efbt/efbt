/**
 */
package org.eclipse.efbt.cocalimo.core.model.task;


/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Parallel Choice Task</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * A TaskSet, where the sub Tasks can be run in parallel , or in any order, since there is no dependency between them whereby one needs to run before another. ParallelChoiceTask represents the case where one or more of the contained moduels are run, unlike ParallelTask where all should be run.
 * <!-- end-model-doc -->
 *
 *
 * @see org.eclipse.efbt.cocalimo.core.model.task.TaskPackage#getParallelChoiceTask()
 * @model extendedMetaData="name='ParallelChoiceTask' kind='elementOnly'"
 * @generated
 */
public interface ParallelChoiceTask extends TaskSet {
} // ParallelChoiceTask
