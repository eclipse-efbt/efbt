package org.eclipse.efbt.aorta.component.report_structures.java_interface;

import java.util.List;



import report_structures.RegReportModule;

public interface ReportStructures {
	
	public List<RegReportModule>  getModules();
	public RegReportModule  getModuleForVerion(String version);

}
