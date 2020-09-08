/*
 * #%L
 * org.eclipse.emf.eson
 * %%
 * Copyright (C) 2002 - 2014 Sebastian Benz, Michael Vorburger and others
 * %%
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 * #L%
 */
package org.eclipse.emf.eson.building;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;

import org.eclipse.emf.common.util.Enumerator;
import org.eclipse.emf.ecore.EEnumLiteral;

public class ValueSwitch extends AbstractValueSwitch<Object> {

	@Override
	protected Object caseLong(Long value) {
		return value;
	}

	@Override
	protected Object caseDouble(Double value) {
		return value;
	}

	@Override
	protected Object caseBigDecimal(BigDecimal value) {
		return value;
	}

	@Override
	protected Object caseBigInteger(BigInteger value) {
		return value;
	}

	@Override
	protected Object caseShort(Short value) {
		return value;
	}

	@Override
	protected Object caseInt(Integer value) {
		return value;
	}

	@Override
	protected Object caseEnum(EEnumLiteral value) {
		if (value.eIsProxy()) // @see AttributeBuilder & BrokenEnumTest
			throw new IllegalArgumentException("EEnumLiteral.eIsProxy(): " + value.eResource().getURI());
		return value.getInstance();
	}

	@Override
	protected Object caseBoolean(Boolean value) {
		return value;
	}

	@Override
	protected Object caseString(String value) {
		return value;
	}

	@Override
	protected Object caseDate(Date value) {
		return value;
	}

	@Override
	protected Object caseEnum(Enumerator value) {
		return value;
	}

}
