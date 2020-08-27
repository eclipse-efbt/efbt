/*
 * #%L
 * org.eclipse.emf.eson
 * %%
 * Copyright (C) 2015 - 2016 Michael Vorburger and others.
 * %%
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 * #L%
 */
package org.eclipse.emf.eson.xtextbackpatch;

import org.eclipse.emf.ecore.resource.ResourceSet;
import org.eclipse.xtext.resource.containers.IAllContainersState;
import org.eclipse.xtext.resource.containers.ResourceSetBasedAllContainersStateProvider;

/**
 * Provider for FasterFlatResourceSetBasedAllContainersState.
 *
 * TODO Remove this class once ESON is migrated to support only Xtext 2.9.0 and no 2.8.x anymore.
 *
 * @see FasterFlatResourceSetBasedAllContainersState
 *
 * @author Michael Vorburger
 */
public class FasterResourceSetBasedAllContainersStateProvider extends ResourceSetBasedAllContainersStateProvider {

    @Override
    protected IAllContainersState handleAdapterNotFound(ResourceSet resourceSet) {
        return new FasterFlatResourceSetBasedAllContainersState(resourceSet);
    }

}
