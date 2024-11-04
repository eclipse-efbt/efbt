# coding=UTF-8
# Copyright (c) 2024 Bird Software Solutions Ltd
# This program and the accompanying materials
# are made available under the terms of the Eclipse Public License 2.0
# which accompanies this distribution, and is available at
# https://www.eclipse.org/legal/epl-2.0/
#
# SPDX-License-Identifier: EPL-2.0
#
# Contributors:
#    Neil Mackenzie - initial API and implementation


from django.contrib import admin


from .sdd_models import DOMAIN				
admin.site.register(DOMAIN)
from .sdd_models import SUBDOMAIN				
admin.site.register(SUBDOMAIN)



from .sdd_models import FACET_COLLECTION				
admin.site.register(FACET_COLLECTION)
from .sdd_models import MAINTENANCE_AGENCY				
admin.site.register(MAINTENANCE_AGENCY)
from .sdd_models import MEMBER				
admin.site.register(MEMBER)
from .sdd_models import MEMBER_HIERARCHY				
admin.site.register(MEMBER_HIERARCHY)
from .sdd_models import MEMBER_HIERARCHY_NODE				
admin.site.register(MEMBER_HIERARCHY_NODE)
from .sdd_models import VARIABLE				
admin.site.register(VARIABLE)
from .sdd_models import FRAMEWORK				
admin.site.register(FRAMEWORK)
from .sdd_models import MEMBER_MAPPING				
admin.site.register(MEMBER_MAPPING)
from .sdd_models import MEMBER_MAPPING_ITEM				
admin.site.register(MEMBER_MAPPING_ITEM)
from .sdd_models import VARIABLE_MAPPING_ITEM				
admin.site.register(VARIABLE_MAPPING_ITEM)
from .sdd_models import VARIABLE_MAPPING				
admin.site.register(VARIABLE_MAPPING)
from .sdd_models import MAPPING_TO_CUBE				
admin.site.register(MAPPING_TO_CUBE)
from .sdd_models import MAPPING_DEFINITION				
admin.site.register(MAPPING_DEFINITION)
from .sdd_models import AXIS				
admin.site.register(AXIS)
from .sdd_models import AXIS_ORDINATE				
admin.site.register(AXIS_ORDINATE)
from .sdd_models import CELL_POSITION				
admin.site.register(CELL_POSITION)
from .sdd_models import ORDINATE_ITEM				
admin.site.register(ORDINATE_ITEM)
from .sdd_models import TABLE				
admin.site.register(TABLE)
from .sdd_models import TABLE_CELL				
admin.site.register(TABLE_CELL)
from .sdd_models import CUBE
admin.site.register(CUBE)
from .sdd_models import CUBE_STRUCTURE     
admin.site.register(CUBE_STRUCTURE)
from .sdd_models import CUBE_STRUCTURE_ITEM
admin.site.register(CUBE_STRUCTURE_ITEM)
from .sdd_models import CUBE_LINK
admin.site.register(CUBE_LINK)
from .sdd_models import CUBE_STRUCTURE_ITEM_LINK
admin.site.register(CUBE_STRUCTURE_ITEM_LINK)
from .sdd_models import COMBINATION
admin.site.register(COMBINATION)
from .sdd_models import COMBINATION_ITEM
admin.site.register(COMBINATION_ITEM)
from .sdd_models import CUBE_TO_COMBINATION
admin.site.register(CUBE_TO_COMBINATION)

from .ldm_models import ABSTRCT_INSTRMNT_RL
admin.site.register(ABSTRCT_INSTRMNT_RL)
from .ldm_models import ASST_PL
admin.site.register(ASST_PL)
from .ldm_models import ASST_PL_DBT_SCRTY_PSTN_ASSGNMNT
admin.site.register(ASST_PL_DBT_SCRTY_PSTN_ASSGNMNT)
from .ldm_models import ASST_PL_EQT_INSTRMNT_NT_SCRT_ASSGNMNT
admin.site.register(ASST_PL_EQT_INSTRMNT_NT_SCRT_ASSGNMNT)
from .ldm_models import ASST_PL_LN_ASSGNMNT
admin.site.register(ASST_PL_LN_ASSGNMNT)
from .ldm_models import BLNC_SHT_NTTNG
admin.site.register(BLNC_SHT_NTTNG)
from .ldm_models import CLLTRL
admin.site.register(CLLTRL)
from .ldm_models import CLLTRL_NN_FNNCL_ASST_ASSGNMNT
admin.site.register(CLLTRL_NN_FNNCL_ASST_ASSGNMNT)
from .ldm_models import CRDT_FCLTY
admin.site.register(CRDT_FCLTY)
from .ldm_models import CRDT_FCLTY_CLLTRL_ASSGNMNT
admin.site.register(CRDT_FCLTY_CLLTRL_ASSGNMNT)
from .ldm_models import CRDT_FCLTY_CLLTRL_RCVD_INSTRMNT_ASSGNMNT
admin.site.register(CRDT_FCLTY_CLLTRL_RCVD_INSTRMNT_ASSGNMNT)
from .ldm_models import CRDT_FCLTY_ENTTY_RL_ASSGNMNT
admin.site.register(CRDT_FCLTY_ENTTY_RL_ASSGNMNT)
from .ldm_models import CRDT_RSK_MTGTN_ASSGNMNT
admin.site.register(CRDT_RSK_MTGTN_ASSGNMNT)
from .ldm_models import CRDT_TRNSFR_OTHR_SCRTSTN_CVRD_BND_PRGRM
admin.site.register(CRDT_TRNSFR_OTHR_SCRTSTN_CVRD_BND_PRGRM)
from .ldm_models import CSH_HND
admin.site.register(CSH_HND)
from .ldm_models import CVRD_BND_ISSNC
admin.site.register(CVRD_BND_ISSNC)
from .ldm_models import CVRD_BND_PRGRM
admin.site.register(CVRD_BND_PRGRM)
from .ldm_models import DBT_SCRTY_ISSD
admin.site.register(DBT_SCRTY_ISSD)
from .ldm_models import DBT_SCRTY_ISSD_TRDTNL_SCRTSTN_ASSGNMNT
admin.site.register(DBT_SCRTY_ISSD_TRDTNL_SCRTSTN_ASSGNMNT)
from .ldm_models import DBT_SCRTY_PRTCTN_ARRNGMNT_ASSGNMNT
admin.site.register(DBT_SCRTY_PRTCTN_ARRNGMNT_ASSGNMNT)
from .ldm_models import ENTTY_RL
admin.site.register(ENTTY_RL)
from .ldm_models import EQT_INSTRMNT_LG_EQT_INSTRMNT_NT_SCRT_ASSGNMNT
admin.site.register(EQT_INSTRMNT_LG_EQT_INSTRMNT_NT_SCRT_ASSGNMNT)
from .ldm_models import ETD_LBLTY_PSTN_SNTHTC_SCRTSTN_ASSGNMNT
admin.site.register(ETD_LBLTY_PSTN_SNTHTC_SCRTSTN_ASSGNMNT)
from .ldm_models import EXCHNG_TRDBL_DRVTV_POSTN_RL
admin.site.register(EXCHNG_TRDBL_DRVTV_POSTN_RL)
from .ldm_models import EXCHNG_TRDBL_DRVTV_PSTN
admin.site.register(EXCHNG_TRDBL_DRVTV_PSTN)
from .ldm_models import FNDMNTL_RVW_TRDNG_BK_STNDRD_APPRCH_RSK_MSR_ETD_PSTNS
admin.site.register(FNDMNTL_RVW_TRDNG_BK_STNDRD_APPRCH_RSK_MSR_ETD_PSTNS)
from .ldm_models import FNDMNTL_RVW_TRDNG_BK_STNDRD_APPRCH_RSK_MSR_FR_SCRTY_PSTNS
admin.site.register(FNDMNTL_RVW_TRDNG_BK_STNDRD_APPRCH_RSK_MSR_FR_SCRTY_PSTNS)
from .ldm_models import FNDMNTL_RVW_TRDNG_BK_STNDRD_APPRCH_RSK_MSR_OTC_PSTNS
admin.site.register(FNDMNTL_RVW_TRDNG_BK_STNDRD_APPRCH_RSK_MSR_OTC_PSTNS)
from .ldm_models import FNNCL_CNTRCT
admin.site.register(FNNCL_CNTRCT)
from .ldm_models import FNNCL_GRNT_INSTRMNT_DBT_SCRT_DBT_SCRTY_ASSGNMNT
admin.site.register(FNNCL_GRNT_INSTRMNT_DBT_SCRT_DBT_SCRTY_ASSGNMNT)
from .ldm_models import GRP
admin.site.register(GRP)
from .ldm_models import GRP_CLNTS_KY_MNGMNT_PRSNLL_ASSGNMNT
admin.site.register(GRP_CLNTS_KY_MNGMNT_PRSNLL_ASSGNMNT)
from .ldm_models import IMMDT_PRNT_ENTRPRS_ASSGNMNT
admin.site.register(IMMDT_PRNT_ENTRPRS_ASSGNMNT)
from .ldm_models import INSTRMNT
admin.site.register(INSTRMNT)
from .ldm_models import INSTRMNT_CLLTRL_RCVD_INSTRMNT_ASSGNMNT
admin.site.register(INSTRMNT_CLLTRL_RCVD_INSTRMNT_ASSGNMNT)
from .ldm_models import INSTRMNT_ENTTY_RL_ASSGNMNT
admin.site.register(INSTRMNT_ENTTY_RL_ASSGNMNT)
from .ldm_models import INSTRMNT_HDGD_EXCHNG_TRDBL_DRVTV
admin.site.register(INSTRMNT_HDGD_EXCHNG_TRDBL_DRVTV)
from .ldm_models import INSTRMNT_HDGD_OTC_DRVTV
admin.site.register(INSTRMNT_HDGD_OTC_DRVTV)
from .ldm_models import INSTRMNT_PRTCN_ARRNGMNT_ASSGNMNT
admin.site.register(INSTRMNT_PRTCN_ARRNGMNT_ASSGNMNT)
from .ldm_models import INTRNL_GRP_KY_MNGMNT_PRSNLL_ASSGNMNT
admin.site.register(INTRNL_GRP_KY_MNGMNT_PRSNLL_ASSGNMNT)
from .ldm_models import INTRNL_GRP_RL
admin.site.register(INTRNL_GRP_RL)
from .ldm_models import INTRST_RT_RSK_HDG_PRTFL
admin.site.register(INTRST_RT_RSK_HDG_PRTFL)
from .ldm_models import KB_PR_BCKT
admin.site.register(KB_PR_BCKT)
from .ldm_models import LN_AND_ADVNC_LG_LN_AND_ADVNC_ASSGNMNT
admin.site.register(LN_AND_ADVNC_LG_LN_AND_ADVNC_ASSGNMNT)
from .ldm_models import LN_EXCLDNG_RPRCHS_AGRMNT_CLLTRL_ASSGNMNT
admin.site.register(LN_EXCLDNG_RPRCHS_AGRMNT_CLLTRL_ASSGNMNT)
from .ldm_models import LNG_DBT_SCRTY_PSTN_PRDNTL_PRTFL_ASSGNMNT_ENCMBRNC_DT
admin.site.register(LNG_DBT_SCRTY_PSTN_PRDNTL_PRTFL_ASSGNMNT_ENCMBRNC_DT)
from .ldm_models import LNG_EQTY_FND_SCRTY_PSTN_PRDNTL_PRTFL_ASSGNMNT_ENCMBRNC_DT
admin.site.register(LNG_EQTY_FND_SCRTY_PSTN_PRDNTL_PRTFL_ASSGNMNT_ENCMBRNC_DT)
from .ldm_models import LNG_NN_NGTBL_SCRTY_PSTN_CLLTRL_ASSGNMNT
admin.site.register(LNG_NN_NGTBL_SCRTY_PSTN_CLLTRL_ASSGNMNT)
from .ldm_models import LNG_SCRTY_PSTN_PRDNTL_PRTFL_ASSGNMNT
admin.site.register(LNG_SCRTY_PSTN_PRDNTL_PRTFL_ASSGNMNT)
from .ldm_models import LNG_SCRTY_PSTN_PRDNTL_PRTFL_ASSGNMNT_ACCNTNG_CLSSFCTN_FNNCL_ASSTS_ASSGNMNT
admin.site.register(LNG_SCRTY_PSTN_PRDNTL_PRTFL_ASSGNMNT_ACCNTNG_CLSSFCTN_FNNCL_ASSTS_ASSGNMNT)
from .ldm_models import LNG_SCRTY_PSTN_PRDNTL_PRTFL_ASSGNMNT_RSK_DT
admin.site.register(LNG_SCRTY_PSTN_PRDNTL_PRTFL_ASSGNMNT_RSK_DT)
from .ldm_models import LNKD_ENTRPRS_ASSGNMNT
admin.site.register(LNKD_ENTRPRS_ASSGNMNT)
from .ldm_models import MSTR_AGRMNT
admin.site.register(MSTR_AGRMNT)
from .ldm_models import MSTR_AGRMNT_ENTTY_RL_ASSGNMNT
admin.site.register(MSTR_AGRMNT_ENTTY_RL_ASSGNMNT)
from .ldm_models import MSTR_AGRMNT_FNNCL_CNTRCT_ASSGNMNT
admin.site.register(MSTR_AGRMNT_FNNCL_CNTRCT_ASSGNMNT)
from .ldm_models import NN_FNNCL_ASST
admin.site.register(NN_FNNCL_ASST)
from .ldm_models import NN_FNNCL_LBLTY
admin.site.register(NN_FNNCL_LBLTY)
from .ldm_models import NTRL_PRSN_KY_MNGMNT_PRSNLL_ASSGNMNT
admin.site.register(NTRL_PRSN_KY_MNGMNT_PRSNLL_ASSGNMNT)
from .ldm_models import OFF_BLNC_INSTRMNT_CLLTRL_ASSGNMNT
admin.site.register(OFF_BLNC_INSTRMNT_CLLTRL_ASSGNMNT)
from .ldm_models import OTC_DRVTV_INSTRMNT_SNTHTC_SCRTSTN_ASSGNMNT
admin.site.register(OTC_DRVTV_INSTRMNT_SNTHTC_SCRTSTN_ASSGNMNT)
from .ldm_models import OTHR_PRTY_ID
admin.site.register(OTHR_PRTY_ID)
from .ldm_models import PRTCTN_ARRNGMNT
admin.site.register(PRTCTN_ARRNGMNT)
from .ldm_models import PRTCTN_PRTCTN_PRVD_ASSGNMNT
admin.site.register(PRTCTN_PRTCTN_PRVD_ASSGNMNT)
from .ldm_models import PRTNR_ENTRPRS_ASSGNMNT
admin.site.register(PRTNR_ENTRPRS_ASSGNMNT)
from .ldm_models import PRTY
admin.site.register(PRTY)
from .ldm_models import PRTY_CD
admin.site.register(PRTY_CD)
from .ldm_models import PRTY_PRVS_PRD_DT
admin.site.register(PRTY_PRVS_PRD_DT)
from .ldm_models import RPRCHS_AGRMNT_CMPNNT
admin.site.register(RPRCHS_AGRMNT_CMPNNT)
from .ldm_models import RSK_FAC_SA
admin.site.register(RSK_FAC_SA)
from .ldm_models import RTNG_AGNCY
admin.site.register(RTNG_AGNCY)
from .ldm_models import RTNG_GRD
admin.site.register(RTNG_GRD)
from .ldm_models import RTNG_GRD_CNTRY_ASSGNMNT
admin.site.register(RTNG_GRD_CNTRY_ASSGNMNT)
from .ldm_models import RTNG_GRD_ISS_BSD_RTNG_SSTM_DBT_SCRTY_ASSGNMNT
admin.site.register(RTNG_GRD_ISS_BSD_RTNG_SSTM_DBT_SCRTY_ASSGNMNT)
from .ldm_models import RTNG_GRD_OTHR_ORGNSTN_ASSGNMNT
admin.site.register(RTNG_GRD_OTHR_ORGNSTN_ASSGNMNT)
from .ldm_models import RTNG_SYSTM
admin.site.register(RTNG_SYSTM)
from .ldm_models import RTNG_SYSTM_APPLD_LGL_PRSN
admin.site.register(RTNG_SYSTM_APPLD_LGL_PRSN)
from .ldm_models import SBSDRY_JNT_VNTR_ASSCT_OTHR_ORGNSTN_ASSGNMNT
admin.site.register(SBSDRY_JNT_VNTR_ASSCT_OTHR_ORGNSTN_ASSGNMNT)
from .ldm_models import SCRTY_ENTTY_RL_ASSGNMNT
admin.site.register(SCRTY_ENTTY_RL_ASSGNMNT)
from .ldm_models import SCRTY_EXCHNG_TRDBL_DRVTV
admin.site.register(SCRTY_EXCHNG_TRDBL_DRVTV)
from .ldm_models import SCRTY_HDGD_EXCHNG_TRDBL_DRVTV
admin.site.register(SCRTY_HDGD_EXCHNG_TRDBL_DRVTV)
from .ldm_models import SCRTY_PSTN
admin.site.register(SCRTY_PSTN)
from .ldm_models import SCRTY_PSTN_HDGD_OTC_DRVTV
admin.site.register(SCRTY_PSTN_HDGD_OTC_DRVTV)
from .ldm_models import SCRTY_SCRTY_RPRCHS_AGRMNT_CMPNNT_ASSGNMNT
admin.site.register(SCRTY_SCRTY_RPRCHS_AGRMNT_CMPNNT_ASSGNMNT)
from .ldm_models import SCTRY_BRRWNG_LNDNG_TRNSCTN_INCLDNG_CSH_CLLTRL
admin.site.register(SCTRY_BRRWNG_LNDNG_TRNSCTN_INCLDNG_CSH_CLLTRL)
from .ldm_models import SHRT_SCRTY_PSTN_PRDNTL_PRTFL_ASSGNMNT
admin.site.register(SHRT_SCRTY_PSTN_PRDNTL_PRTFL_ASSGNMNT)
from .ldm_models import SNTHTC_SCRTSTN
admin.site.register(SNTHTC_SCRTSTN)
from .ldm_models import SYNDCTD_CNTRCT
admin.site.register(SYNDCTD_CNTRCT)
from .ldm_models import TRDTNL_SCRTSTN
admin.site.register(TRDTNL_SCRTSTN)
from .ldm_models import TRNCH_SYNTHTC_SCRTSTN_WTHT_SSPE_DPST
admin.site.register(TRNCH_SYNTHTC_SCRTSTN_WTHT_SSPE_DPST)
from .ldm_models import TRNCH_SYNTHTC_SCRTSTN_WTHT_SSPE_FNNCL_GRNT
admin.site.register(TRNCH_SYNTHTC_SCRTSTN_WTHT_SSPE_FNNCL_GRNT)
from .ldm_models import TRNCH_TRDTNL_SCRTSTN
admin.site.register(TRNCH_TRDTNL_SCRTSTN)

