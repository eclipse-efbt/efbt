package org.eclipse.efbt.cocason.dsl.cocason.conversion;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;


import org.eclipse.xtext.common.services.Ecore2XtextTerminalConverters;
import org.eclipse.xtext.conversion.IValueConverter;
import org.eclipse.xtext.conversion.ValueConverter;
import org.eclipse.xtext.conversion.ValueConverterException;
import org.eclipse.xtext.nodemodel.INode;
import org.eclipse.xtext.util.Strings;

public class DATEValueConverter extends Ecore2XtextTerminalConverters
{
	

@ValueConverter(rule = "EDate")
public IValueConverter<Date> EDate() {
    return new IValueConverter<Date>() {

        @Override
        public Date toValue(String string, INode node) throws ValueConverterException {
        	if (Strings.isEmpty(string))
    			throw new ValueConverterException("Couldn't convert empty string to a date value.", node, null);
    		try {
    			SimpleDateFormat dateFormat = new SimpleDateFormat("MM.dd.yyyy"); // see above  
    			return dateFormat.parse(string);
    		} catch (ParseException e) {
    			throw new ValueConverterException("Couldn't convert '" + string + "' to a date value.", node, e);
    		}
        }

        @Override
        public String toString(Date value) throws ValueConverterException {
        	if (value == null) {
    			throw new ValueConverterException("Value may not be null.", null, null);
    		}
    		SimpleDateFormat dateFormat = new SimpleDateFormat("MM.dd.yyyy"); // see above  
    		return dateFormat.format(value);
        }
    };
    
}
}

