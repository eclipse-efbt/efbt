/*
 * #%L
 * org.eclipse.emf.eson
 * %%
 * Copyright (C) 2013 - 2015 Michael Vorburger, Anton Kosyakov 
 * %%
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 * #L%
 */
package org.eclipse.emf.eson.conversion;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.eclipse.xtext.conversion.ValueConverterException;
import org.eclipse.xtext.conversion.impl.AbstractValueConverter;
import org.eclipse.xtext.nodemodel.INode;
import org.eclipse.xtext.util.Strings;

import com.google.inject.Inject;
import com.google.inject.name.Named;

public class DATEValueConverter extends AbstractValueConverter<Date> {
	// NOTE Data Type IValueConverter must extend AbstractValueConverter and not AbstractLexerBasedConverter like Terminals
	
	public static final String DATE_FORMAT_KEY = "date_format";

	@Inject
	@Named(value = DATE_FORMAT_KEY)
	private String dateFormatPattern = null;

	// NOTE We store the Pattern, but not the SimpleDateFormat,
	// as that is not thread safe (an IValueConverter may be used multi-threaded).
	// (Using org.apache.commons.lang.time.FastDateFormat would be an alternative,
	// but this approach makes ESON independent of Commons Lang from Orbit.)
	
	//@Override
	public String toString(Date value) throws ValueConverterException {
		if (value == null) {
			throw new ValueConverterException("Value may not be null.", null, null);
		}
		SimpleDateFormat dateFormat = new SimpleDateFormat(dateFormatPattern); // see above  
		return dateFormat.format(value);
	}
	
	//@Override
	public Date toValue(String string, INode node) {
		if (Strings.isEmpty(string))
			throw new ValueConverterException("Couldn't convert empty string to a date value.", node, null);
		try {
			SimpleDateFormat dateFormat = new SimpleDateFormat(dateFormatPattern); // see above  
			return dateFormat.parse(string);
		} catch (ParseException e) {
			throw new ValueConverterException("Couldn't convert '" + string + "' to a date value.", node, e);
		}
	}

}
