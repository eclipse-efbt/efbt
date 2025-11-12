   ╭───────────────────────────────────────────────────────╮
     │ Fix DPM Member Mapping - Handle Missing Domains from  │
     │ XBRL Codes                                            │
     │                                                       │
     │ Problem                                               │
     │                                                       │
     │ When mapping members from DPM, if a domain is not     │
     │ found in the domain mapping, the system should:       │
     │ 1. Parse MEMBER_XBRL_CODE (format:                    │
     │ DomainCode:MemberCode, e.g., eba_AP:x1)               │
     │ 2. Split at : → left = Domain ID, right = Member Code │
     │ 3. Create/use that domain                             │
     │ 4. Set member ID to DomainID_MemberCode format        │
     │                                                       │
     │ Changes Required                                      │
     │                                                       │
     │ File: birds_nest/pybirdai/process_steps/dpm_integratio│
     │ n/mapping_functions/members.py                        │
     │                                                       │
     │ Step 1: Add logic after line 29 to extract domain from│
     │  XBRL code when domain mapping fails:                 │
     │ - Check if DOMAIN_ID is empty/NaN or not in           │
     │ domain_id_map                                         │
     │ - If so, parse MEMBER_XBRL_CODE by splitting at :     │
     │ - Use left side as the domain identifier              │
     │                                                       │
     │ Step 2: Modify member ID creation logic (around line  │
     │ 36):                                                  │
     │ - For members where domain comes from XBRL code: use  │
     │ DomainID_MemberCode format                            │
     │ - For members with mapped domains: keep existing      │
     │ MAPPED_DOMAIN_ID_EBA_MEMBER_CODE format               │
     │                                                       │
     │ Step 3: Track domains that need to be created:        │
     │ - Return information about new domains derived from   │
     │ XBRL codes                                            │
     │ - These may need to be created in the domain mapping  │
     │ process                                               │
     │                                                       │
     │ Step 4: Fix any column duplication issues in final    │
     │ output (line 52)                                      │
     │                                                       │
     │ This ensures members without mapped domains can still │
     │ be processed by inferring domain information from     │
     │ their XBRL codes.                                     │
     ╰───────────────────────────────────────────────────────╯
