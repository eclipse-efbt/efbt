package org.eclipse.efbt.aorta.component.report_based_functional_domain_breakdown.java_interface;

import java.util.List;



import report_based_report_based_functional_domain_breakdown.ReportBasedFunctionalityModuleModule;
import report_structures.RegReportModule;
import requirements_text.RequirementsModule;

public interface ReportBasedFunctionalDomainBreakdown {

	public List<ReportBasedFunctionalityModuleModule>  getModules();
	public ReportBasedFunctionalityModuleModule  getModuleForVerion(String version);
	public void createTrialVersion(ReportBasedFunctionalityModuleModule oldVersion,
			RequirementsModule dependantRequirementsVersion,
			RegReportModule dependantOutputStructuresVersion,
			String version );
	public void setTrialModuleAsFinal(String version);
	public void removeInvalidItemsFromTrialModule(String version);
}
