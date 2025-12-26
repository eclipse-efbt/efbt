/**
 * Artifact Diff Modal - JavaScript for comparing linked artifacts
 *
 * This module handles the comparison of CUBE_LINK, CUBE_STRUCTURE_ITEM_LINK,
 * and MEMBER_LINK between CSV files and database state.
 */

// Store the current change report for use in PR creation
let currentChangeReport = null;

/**
 * Open the artifact diff modal and fetch comparison data
 * @param {string} frameworkId - Optional framework ID for filtering
 * @param {string} csvDir - Optional CSV directory path
 */
function openArtifactDiffModal(frameworkId = null, csvDir = null) {
    const modal = document.getElementById('artifactDiffModal');
    if (!modal) {
        console.error('Artifact diff modal not found');
        return;
    }

    // Show modal
    modal.style.display = 'flex';

    // Reset states
    document.getElementById('artifactDiffLoading').style.display = 'block';
    document.getElementById('artifactDiffError').style.display = 'none';
    document.getElementById('artifactDiffNoChanges').style.display = 'none';
    document.getElementById('artifactDiffSummary').style.display = 'none';
    document.getElementById('proceedToPushBtn').disabled = true;

    // Build URL with query parameters
    let url = '/pybirdai/workflow/compare-linked-artifacts/';
    const params = new URLSearchParams();
    if (frameworkId) params.append('framework_id', frameworkId);
    if (csvDir) params.append('csv_dir', csvDir);
    if (params.toString()) url += '?' + params.toString();

    // Fetch comparison data
    fetch(url)
        .then(response => response.json())
        .then(data => {
            document.getElementById('artifactDiffLoading').style.display = 'none';

            if (!data.success) {
                showArtifactDiffError(data.error || 'Unknown error occurred');
                return;
            }

            currentChangeReport = data;

            if (!data.has_changes) {
                document.getElementById('artifactDiffNoChanges').style.display = 'block';
                return;
            }

            // Show summary
            displayArtifactDiffSummary(data);
        })
        .catch(error => {
            document.getElementById('artifactDiffLoading').style.display = 'none';
            showArtifactDiffError(error.message);
        });
}

/**
 * Close the artifact diff modal
 */
function closeArtifactDiffModal() {
    const modal = document.getElementById('artifactDiffModal');
    if (modal) {
        modal.style.display = 'none';
    }
    currentChangeReport = null;
}

/**
 * Show an error message in the modal
 * @param {string} message - Error message to display
 */
function showArtifactDiffError(message) {
    document.getElementById('artifactDiffError').style.display = 'block';
    document.getElementById('artifactDiffErrorMessage').textContent = message;
}

/**
 * Display the artifact diff summary
 * @param {Object} data - The comparison data from the API
 */
function displayArtifactDiffSummary(data) {
    const summary = data.summary;

    // Show summary section
    document.getElementById('artifactDiffSummary').style.display = 'block';

    // Update CUBE_LINK counts
    if (summary.cube_link) {
        document.getElementById('cubeLinkNew').textContent = summary.cube_link.new_count || 0;
        document.getElementById('cubeLinkMod').textContent = summary.cube_link.modified_count || 0;
        document.getElementById('cubeLinkDel').textContent = summary.cube_link.deleted_count || 0;
    }

    // Update CUBE_STRUCTURE_ITEM_LINK counts
    if (summary.cube_structure_item_link) {
        document.getElementById('csilNew').textContent = summary.cube_structure_item_link.new_count || 0;
        document.getElementById('csilMod').textContent = summary.cube_structure_item_link.modified_count || 0;
        document.getElementById('csilDel').textContent = summary.cube_structure_item_link.deleted_count || 0;
    }

    // Update MEMBER_LINK counts
    if (summary.member_link) {
        document.getElementById('memberLinkNew').textContent = summary.member_link.new_count || 0;
        document.getElementById('memberLinkMod').textContent = summary.member_link.modified_count || 0;
        document.getElementById('memberLinkDel').textContent = summary.member_link.deleted_count || 0;
    }

    // Update validation status
    updateValidationStatus(summary.validation);

    // Populate detail lists
    populateDetailsList('cubeLinkDetailsList', data.details.cube_link);
    populateDetailsList('csilDetailsList', data.details.cube_structure_item_link);
    populateDetailsList('memberLinkDetailsList', data.details.member_link);

    // Enable proceed button if validation passes
    if (!summary.validation || summary.validation.all_valid) {
        document.getElementById('proceedToPushBtn').disabled = false;
    }
}

/**
 * Update the validation status display
 * @param {Object} validation - Validation summary from the API
 */
function updateValidationStatus(validation) {
    const container = document.getElementById('artifactValidationStatus');

    if (!validation || validation.all_valid) {
        container.style.background = '#f0fff4';
        container.style.border = '1px solid #9ae6b4';
        container.innerHTML = `
            <div style="display: flex; align-items: center; gap: 10px; color: #276749;">
                <i class="fas fa-check-circle" style="font-size: 20px;"></i>
                <div>
                    <strong>Validation Passed</strong>
                    <p style="margin: 5px 0 0 0; font-size: 13px; color: #48bb78;">
                        All ${validation ? validation.total_checked : 0} artifacts validated successfully.
                    </p>
                </div>
            </div>
        `;
    } else {
        container.style.background = '#fffaf0';
        container.style.border = '1px solid #fbd38d';
        container.innerHTML = `
            <div style="display: flex; align-items: center; gap: 10px; color: #c05621;">
                <i class="fas fa-exclamation-triangle" style="font-size: 20px;"></i>
                <div>
                    <strong>Validation Issues Found</strong>
                    <p style="margin: 5px 0 0 0; font-size: 13px; color: #dd6b20;">
                        ${validation.total_invalid} of ${validation.total_checked} artifacts have validation errors.
                        These will be skipped during import.
                    </p>
                </div>
            </div>
            <div style="margin-top: 10px; max-height: 150px; overflow-y: auto;">
                ${validation.invalid_artifacts.map(a => `
                    <div style="padding: 8px; background: white; border-radius: 4px; margin-bottom: 5px; font-size: 12px;">
                        <strong>${a.type}</strong>: ${a.id}
                        <ul style="margin: 5px 0 0 0; padding-left: 20px; color: #e53e3e;">
                            ${a.errors.map(e => `<li>${e}</li>`).join('')}
                        </ul>
                    </div>
                `).join('')}
            </div>
        `;
    }
}

/**
 * Populate a details list with artifact changes
 * @param {string} listId - The ID of the list container
 * @param {Object} changes - Object with new, modified, deleted arrays
 */
function populateDetailsList(listId, changes) {
    const container = document.getElementById(listId);
    if (!container) return;

    const hasChanges = (changes.new && changes.new.length > 0) ||
                       (changes.modified && changes.modified.length > 0) ||
                       (changes.deleted && changes.deleted.length > 0);

    if (!hasChanges) {
        container.innerHTML = '<p style="color: #a0aec0; font-style: italic;">No changes</p>';
        return;
    }

    let html = '';

    // New artifacts
    if (changes.new && changes.new.length > 0) {
        html += `
            <div style="margin-bottom: 15px;">
                <h5 style="color: #38a169; margin: 0 0 10px 0; font-size: 14px;">
                    <i class="fas fa-plus"></i> New (${changes.new.length})
                </h5>
                <div style="display: flex; flex-wrap: wrap; gap: 5px;">
                    ${changes.new.map(a => `
                        <span style="background: #c6f6d5; color: #276749; padding: 4px 10px; border-radius: 4px; font-size: 12px; font-family: monospace;">
                            ${getArtifactId(a)}
                        </span>
                    `).join('')}
                </div>
            </div>
        `;
    }

    // Modified artifacts
    if (changes.modified && changes.modified.length > 0) {
        html += `
            <div style="margin-bottom: 15px;">
                <h5 style="color: #dd6b20; margin: 0 0 10px 0; font-size: 14px;">
                    <i class="fas fa-edit"></i> Modified (${changes.modified.length})
                </h5>
                <div style="display: flex; flex-wrap: wrap; gap: 5px;">
                    ${changes.modified.map(a => `
                        <span style="background: #feebc8; color: #c05621; padding: 4px 10px; border-radius: 4px; font-size: 12px; font-family: monospace;">
                            ${a.id || a.key || 'unknown'}
                        </span>
                    `).join('')}
                </div>
            </div>
        `;
    }

    // Deleted artifacts
    if (changes.deleted && changes.deleted.length > 0) {
        html += `
            <div>
                <h5 style="color: #e53e3e; margin: 0 0 10px 0; font-size: 14px;">
                    <i class="fas fa-minus"></i> Deleted (${changes.deleted.length})
                </h5>
                <div style="display: flex; flex-wrap: wrap; gap: 5px;">
                    ${changes.deleted.map(a => `
                        <span style="background: #fed7d7; color: #c53030; padding: 4px 10px; border-radius: 4px; font-size: 12px; font-family: monospace;">
                            ${getArtifactId(a)}
                        </span>
                    `).join('')}
                </div>
            </div>
        `;
    }

    container.innerHTML = html;
}

/**
 * Get the display ID for an artifact
 * @param {Object} artifact - The artifact object
 * @returns {string} The ID to display
 */
function getArtifactId(artifact) {
    return artifact.cube_link_id ||
           artifact.cube_structure_item_link_id ||
           artifact.CUBE_LINK_ID ||
           artifact.CUBE_STRUCTURE_ITEM_LINK_ID ||
           artifact.key ||
           artifact.id ||
           'unknown';
}

/**
 * Toggle accordion section visibility
 * @param {string} sectionId - The ID of the section to toggle
 */
function toggleAccordion(sectionId) {
    const section = document.getElementById(sectionId);
    const icon = document.getElementById(sectionId + 'Icon');

    if (!section || !icon) return;

    if (section.style.display === 'none') {
        section.style.display = 'block';
        icon.className = 'fas fa-chevron-up';
    } else {
        section.style.display = 'none';
        icon.className = 'fas fa-chevron-down';
    }
}

/**
 * Proceed to push changes to GitHub
 * This closes the modal and triggers the push workflow with the change report
 */
function proceedToPush() {
    if (!currentChangeReport) {
        console.error('No change report available');
        return;
    }

    // Close this modal
    closeArtifactDiffModal();

    // Store the change report for use by the push workflow
    window.artifactChangeReport = currentChangeReport;

    // Trigger the existing push workflow
    // This assumes there's an existing function to handle GitHub push
    if (typeof openCloneSaveModal === 'function') {
        openCloneSaveModal();
    } else if (typeof showPushToGitHubModal === 'function') {
        showPushToGitHubModal();
    } else {
        console.log('Change report ready for push:', currentChangeReport.summary);
        alert('Change report generated. Ready to push to GitHub.');
    }
}

/**
 * Get the current change report for use in PR description generation
 * @returns {Object|null} The current change report or null
 */
function getArtifactChangeReport() {
    return currentChangeReport || window.artifactChangeReport || null;
}

/**
 * Generate a summary string for PR description
 * @returns {string} Markdown formatted summary
 */
function generateArtifactChangeSummaryForPR() {
    const report = getArtifactChangeReport();
    if (!report || !report.summary) return '';

    const summary = report.summary;
    let md = '\n\n## Linked Artifacts Changed\n\n';

    if (summary.cube_link && summary.cube_link.has_changes) {
        md += `### CUBE_LINK\n`;
        md += `- Added: ${summary.cube_link.new_count || 0}\n`;
        md += `- Modified: ${summary.cube_link.modified_count || 0}\n`;
        md += `- Deleted: ${summary.cube_link.deleted_count || 0}\n\n`;
    }

    if (summary.cube_structure_item_link && summary.cube_structure_item_link.has_changes) {
        md += `### CUBE_STRUCTURE_ITEM_LINK\n`;
        md += `- Added: ${summary.cube_structure_item_link.new_count || 0}\n`;
        md += `- Modified: ${summary.cube_structure_item_link.modified_count || 0}\n`;
        md += `- Deleted: ${summary.cube_structure_item_link.deleted_count || 0}\n\n`;
    }

    if (summary.member_link && summary.member_link.has_changes) {
        md += `### MEMBER_LINK\n`;
        md += `- Added: ${summary.member_link.new_count || 0}\n`;
        md += `- Modified: ${summary.member_link.modified_count || 0}\n`;
        md += `- Deleted: ${summary.member_link.deleted_count || 0}\n\n`;
    }

    if (summary.validation) {
        md += `### Validation Status\n`;
        if (summary.validation.all_valid) {
            md += `All ${summary.validation.total_checked} artifacts validated successfully.\n`;
        } else {
            md += `${summary.validation.total_invalid} of ${summary.validation.total_checked} artifacts have validation errors and will be skipped.\n`;
        }
    }

    return md;
}
