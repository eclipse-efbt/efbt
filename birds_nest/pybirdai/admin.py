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


from .bird_meta_data_model import DOMAIN
admin.site.register(DOMAIN)
from .bird_meta_data_model import SUBDOMAIN
admin.site.register(SUBDOMAIN)
from .bird_meta_data_model import FACET_COLLECTION
admin.site.register(FACET_COLLECTION)
from .bird_meta_data_model import MAINTENANCE_AGENCY
admin.site.register(MAINTENANCE_AGENCY)
from .bird_meta_data_model import MEMBER
admin.site.register(MEMBER)
from .bird_meta_data_model import MEMBER_HIERARCHY
admin.site.register(MEMBER_HIERARCHY)
from .bird_meta_data_model import MEMBER_HIERARCHY_NODE
admin.site.register(MEMBER_HIERARCHY_NODE)
from .bird_meta_data_model import VARIABLE
admin.site.register(VARIABLE)
from .bird_meta_data_model import FRAMEWORK
admin.site.register(FRAMEWORK)
from .bird_meta_data_model import MEMBER_MAPPING
admin.site.register(MEMBER_MAPPING)
from .bird_meta_data_model import MEMBER_MAPPING_ITEM
admin.site.register(MEMBER_MAPPING_ITEM)
from .bird_meta_data_model import VARIABLE_MAPPING_ITEM
admin.site.register(VARIABLE_MAPPING_ITEM)
from .bird_meta_data_model import VARIABLE_MAPPING
admin.site.register(VARIABLE_MAPPING)
from .bird_meta_data_model import MAPPING_TO_CUBE
admin.site.register(MAPPING_TO_CUBE)
from .bird_meta_data_model import MAPPING_DEFINITION
admin.site.register(MAPPING_DEFINITION)
from .bird_meta_data_model import AXIS
admin.site.register(AXIS)
from .bird_meta_data_model import AXIS_ORDINATE
admin.site.register(AXIS_ORDINATE)
from .bird_meta_data_model import CELL_POSITION
admin.site.register(CELL_POSITION)
from .bird_meta_data_model import ORDINATE_ITEM
admin.site.register(ORDINATE_ITEM)
from .bird_meta_data_model import TABLE
admin.site.register(TABLE)
from .bird_meta_data_model import TABLE_CELL
admin.site.register(TABLE_CELL)
from .bird_meta_data_model import CUBE
admin.site.register(CUBE)
from .bird_meta_data_model import CUBE_STRUCTURE
admin.site.register(CUBE_STRUCTURE)
from .bird_meta_data_model import CUBE_STRUCTURE_ITEM
admin.site.register(CUBE_STRUCTURE_ITEM)
from .bird_meta_data_model import CUBE_LINK
admin.site.register(CUBE_LINK)
from .bird_meta_data_model import CUBE_STRUCTURE_ITEM_LINK
admin.site.register(CUBE_STRUCTURE_ITEM_LINK)
from .bird_meta_data_model import COMBINATION
admin.site.register(COMBINATION)
from .bird_meta_data_model import COMBINATION_ITEM
admin.site.register(COMBINATION_ITEM)
from .bird_meta_data_model import CUBE_TO_COMBINATION
admin.site.register(CUBE_TO_COMBINATION)
from .bird_meta_data_model import SUBDOMAIN_ENUMERATION
admin.site.register(SUBDOMAIN_ENUMERATION)
from .bird_meta_data_model import VARIABLE_SET
admin.site.register(VARIABLE_SET)
from .bird_meta_data_model import VARIABLE_SET_ENUMERATION
admin.site.register(VARIABLE_SET_ENUMERATION)
from .bird_meta_data_model import MEMBER_LINK
admin.site.register(MEMBER_LINK)


from .bird_data_model import ABSTRCT_INSTRMNT_RL
admin.site.register(ABSTRCT_INSTRMNT_RL)
from .bird_data_model import ASST_PL
admin.site.register(ASST_PL)
from .bird_data_model import ASST_PL_DBT_SCRTY_PSTN_ASSGNMNT
admin.site.register(ASST_PL_DBT_SCRTY_PSTN_ASSGNMNT)
from .bird_data_model import ASST_PL_EQT_INSTRMNT_NT_SCRT_ASSGNMNT
admin.site.register(ASST_PL_EQT_INSTRMNT_NT_SCRT_ASSGNMNT)
from .bird_data_model import ASST_PL_LN_ASSGNMNT
admin.site.register(ASST_PL_LN_ASSGNMNT)
from .bird_data_model import BLNC_SHT_NTTNG
admin.site.register(BLNC_SHT_NTTNG)
from .bird_data_model import CLLTRL
admin.site.register(CLLTRL)
from .bird_data_model import CLLTRL_NN_FNNCL_ASST_ASSGNMNT
admin.site.register(CLLTRL_NN_FNNCL_ASST_ASSGNMNT)
from .bird_data_model import CRDT_FCLTY
admin.site.register(CRDT_FCLTY)
from .bird_data_model import CRDT_FCLTY_CLLTRL_ASSGNMNT
admin.site.register(CRDT_FCLTY_CLLTRL_ASSGNMNT)
from .bird_data_model import CRDT_FCLTY_CLLTRL_RCVD_INSTRMNT_ASSGNMNT
admin.site.register(CRDT_FCLTY_CLLTRL_RCVD_INSTRMNT_ASSGNMNT)
from .bird_data_model import CRDT_FCLTY_ENTTY_RL_ASSGNMNT
admin.site.register(CRDT_FCLTY_ENTTY_RL_ASSGNMNT)
from .bird_data_model import CRDT_RSK_MTGTN_ASSGNMNT
admin.site.register(CRDT_RSK_MTGTN_ASSGNMNT)
from .bird_data_model import CRDT_TRNSFR_OTHR_SCRTSTN_CVRD_BND_PRGRM
admin.site.register(CRDT_TRNSFR_OTHR_SCRTSTN_CVRD_BND_PRGRM)
from .bird_data_model import CSH_HND
admin.site.register(CSH_HND)
from .bird_data_model import CVRD_BND_ISSNC
admin.site.register(CVRD_BND_ISSNC)
from .bird_data_model import CVRD_BND_PRGRM
admin.site.register(CVRD_BND_PRGRM)
from .bird_data_model import DBT_SCRTY_ISSD
admin.site.register(DBT_SCRTY_ISSD)
from .bird_data_model import DBT_SCRTY_ISSD_TRDTNL_SCRTSTN_ASSGNMNT
admin.site.register(DBT_SCRTY_ISSD_TRDTNL_SCRTSTN_ASSGNMNT)
from .bird_data_model import DBT_SCRTY_PRTCTN_ARRNGMNT_ASSGNMNT
admin.site.register(DBT_SCRTY_PRTCTN_ARRNGMNT_ASSGNMNT)
from .bird_data_model import ENTTY_RL
admin.site.register(ENTTY_RL)
from .bird_data_model import EQT_INSTRMNT_LG_EQT_INSTRMNT_NT_SCRT_ASSGNMNT
admin.site.register(EQT_INSTRMNT_LG_EQT_INSTRMNT_NT_SCRT_ASSGNMNT)
from .bird_data_model import ETD_LBLTY_PSTN_SNTHTC_SCRTSTN_ASSGNMNT
admin.site.register(ETD_LBLTY_PSTN_SNTHTC_SCRTSTN_ASSGNMNT)
from .bird_data_model import EXCHNG_TRDBL_DRVTV_POSTN_RL
admin.site.register(EXCHNG_TRDBL_DRVTV_POSTN_RL)
from .bird_data_model import EXCHNG_TRDBL_DRVTV_PSTN
admin.site.register(EXCHNG_TRDBL_DRVTV_PSTN)
from .bird_data_model import FNDMNTL_RVW_TRDNG_BK_STNDRD_APPRCH_RSK_MSR_ETD_PSTNS
admin.site.register(FNDMNTL_RVW_TRDNG_BK_STNDRD_APPRCH_RSK_MSR_ETD_PSTNS)
from .bird_data_model import FNDMNTL_RVW_TRDNG_BK_STNDRD_APPRCH_RSK_MSR_FR_SCRTY_PSTNS
admin.site.register(FNDMNTL_RVW_TRDNG_BK_STNDRD_APPRCH_RSK_MSR_FR_SCRTY_PSTNS)
from .bird_data_model import FNDMNTL_RVW_TRDNG_BK_STNDRD_APPRCH_RSK_MSR_OTC_PSTNS
admin.site.register(FNDMNTL_RVW_TRDNG_BK_STNDRD_APPRCH_RSK_MSR_OTC_PSTNS)
from .bird_data_model import FNNCL_CNTRCT
admin.site.register(FNNCL_CNTRCT)
from .bird_data_model import FNNCL_GRNT_INSTRMNT_DBT_SCRT_DBT_SCRTY_ASSGNMNT
admin.site.register(FNNCL_GRNT_INSTRMNT_DBT_SCRT_DBT_SCRTY_ASSGNMNT)
from .bird_data_model import GRP
admin.site.register(GRP)
from .bird_data_model import GRP_CLNTS_KY_MNGMNT_PRSNLL_ASSGNMNT
admin.site.register(GRP_CLNTS_KY_MNGMNT_PRSNLL_ASSGNMNT)
from .bird_data_model import IMMDT_PRNT_ENTRPRS_ASSGNMNT
admin.site.register(IMMDT_PRNT_ENTRPRS_ASSGNMNT)
from .bird_data_model import INSTRMNT
admin.site.register(INSTRMNT)
from .bird_data_model import INSTRMNT_CLLTRL_RCVD_INSTRMNT_ASSGNMNT
admin.site.register(INSTRMNT_CLLTRL_RCVD_INSTRMNT_ASSGNMNT)
from .bird_data_model import INSTRMNT_ENTTY_RL_ASSGNMNT
admin.site.register(INSTRMNT_ENTTY_RL_ASSGNMNT)
from .bird_data_model import INSTRMNT_HDGD_EXCHNG_TRDBL_DRVTV
admin.site.register(INSTRMNT_HDGD_EXCHNG_TRDBL_DRVTV)
from .bird_data_model import INSTRMNT_HDGD_OTC_DRVTV
admin.site.register(INSTRMNT_HDGD_OTC_DRVTV)
from .bird_data_model import INSTRMNT_PRTCN_ARRNGMNT_ASSGNMNT
admin.site.register(INSTRMNT_PRTCN_ARRNGMNT_ASSGNMNT)
from .bird_data_model import INTRNL_GRP_KY_MNGMNT_PRSNLL_ASSGNMNT
admin.site.register(INTRNL_GRP_KY_MNGMNT_PRSNLL_ASSGNMNT)
from .bird_data_model import INTRNL_GRP_RL
admin.site.register(INTRNL_GRP_RL)
from .bird_data_model import INTRST_RT_RSK_HDG_PRTFL
admin.site.register(INTRST_RT_RSK_HDG_PRTFL)
from .bird_data_model import KB_PR_BCKT
admin.site.register(KB_PR_BCKT)
from .bird_data_model import LN_AND_ADVNC_LG_LN_AND_ADVNC_ASSGNMNT
admin.site.register(LN_AND_ADVNC_LG_LN_AND_ADVNC_ASSGNMNT)
from .bird_data_model import LN_EXCLDNG_RPRCHS_AGRMNT_CLLTRL_ASSGNMNT
admin.site.register(LN_EXCLDNG_RPRCHS_AGRMNT_CLLTRL_ASSGNMNT)
from .bird_data_model import LNG_DBT_SCRTY_PSTN_PRDNTL_PRTFL_ASSGNMNT_ENCMBRNC_DT
admin.site.register(LNG_DBT_SCRTY_PSTN_PRDNTL_PRTFL_ASSGNMNT_ENCMBRNC_DT)
from .bird_data_model import LNG_EQTY_FND_SCRTY_PSTN_PRDNTL_PRTFL_ASSGNMNT_ENCMBRNC_DT
admin.site.register(LNG_EQTY_FND_SCRTY_PSTN_PRDNTL_PRTFL_ASSGNMNT_ENCMBRNC_DT)
from .bird_data_model import LNG_NN_NGTBL_SCRTY_PSTN_CLLTRL_ASSGNMNT
admin.site.register(LNG_NN_NGTBL_SCRTY_PSTN_CLLTRL_ASSGNMNT)
from .bird_data_model import LNG_SCRTY_PSTN_PRDNTL_PRTFL_ASSGNMNT
admin.site.register(LNG_SCRTY_PSTN_PRDNTL_PRTFL_ASSGNMNT)
from .bird_data_model import LNG_SCRTY_PSTN_PRDNTL_PRTFL_ASSGNMNT_ACCNTNG_CLSSFCTN_FNNCL_ASSTS_ASSGNMNT
admin.site.register(LNG_SCRTY_PSTN_PRDNTL_PRTFL_ASSGNMNT_ACCNTNG_CLSSFCTN_FNNCL_ASSTS_ASSGNMNT)
from .bird_data_model import LNG_SCRTY_PSTN_PRDNTL_PRTFL_ASSGNMNT_RSK_DT
admin.site.register(LNG_SCRTY_PSTN_PRDNTL_PRTFL_ASSGNMNT_RSK_DT)
from .bird_data_model import LNKD_ENTRPRS_ASSGNMNT
admin.site.register(LNKD_ENTRPRS_ASSGNMNT)
from .bird_data_model import MSTR_AGRMNT
admin.site.register(MSTR_AGRMNT)
from .bird_data_model import MSTR_AGRMNT_ENTTY_RL_ASSGNMNT
admin.site.register(MSTR_AGRMNT_ENTTY_RL_ASSGNMNT)
from .bird_data_model import MSTR_AGRMNT_FNNCL_CNTRCT_ASSGNMNT
admin.site.register(MSTR_AGRMNT_FNNCL_CNTRCT_ASSGNMNT)
from .bird_data_model import NN_FNNCL_ASST
admin.site.register(NN_FNNCL_ASST)
from .bird_data_model import NN_FNNCL_LBLTY
admin.site.register(NN_FNNCL_LBLTY)
from .bird_data_model import NTRL_PRSN_KY_MNGMNT_PRSNLL_ASSGNMNT
admin.site.register(NTRL_PRSN_KY_MNGMNT_PRSNLL_ASSGNMNT)
from .bird_data_model import OFF_BLNC_INSTRMNT_CLLTRL_ASSGNMNT
admin.site.register(OFF_BLNC_INSTRMNT_CLLTRL_ASSGNMNT)
from .bird_data_model import OTC_DRVTV_INSTRMNT_SNTHTC_SCRTSTN_ASSGNMNT
admin.site.register(OTC_DRVTV_INSTRMNT_SNTHTC_SCRTSTN_ASSGNMNT)
from .bird_data_model import OTHR_PRTY_ID
admin.site.register(OTHR_PRTY_ID)
from .bird_data_model import PRTCTN_ARRNGMNT
admin.site.register(PRTCTN_ARRNGMNT)
from .bird_data_model import PRTCTN_PRTCTN_PRVD_ASSGNMNT
admin.site.register(PRTCTN_PRTCTN_PRVD_ASSGNMNT)
from .bird_data_model import PRTNR_ENTRPRS_ASSGNMNT
admin.site.register(PRTNR_ENTRPRS_ASSGNMNT)
from .bird_data_model import PRTY
admin.site.register(PRTY)
from .bird_data_model import PRTY_CD
admin.site.register(PRTY_CD)
from .bird_data_model import PRTY_PRVS_PRD_DT
admin.site.register(PRTY_PRVS_PRD_DT)
from .bird_data_model import RPRCHS_AGRMNT_CMPNNT
admin.site.register(RPRCHS_AGRMNT_CMPNNT)
from .bird_data_model import RSK_FAC_SA
admin.site.register(RSK_FAC_SA)
from .bird_data_model import RTNG_AGNCY
admin.site.register(RTNG_AGNCY)
from .bird_data_model import RTNG_GRD
admin.site.register(RTNG_GRD)
from .bird_data_model import RTNG_GRD_CNTRY_ASSGNMNT
admin.site.register(RTNG_GRD_CNTRY_ASSGNMNT)
from .bird_data_model import RTNG_GRD_ISS_BSD_RTNG_SSTM_DBT_SCRTY_ASSGNMNT
admin.site.register(RTNG_GRD_ISS_BSD_RTNG_SSTM_DBT_SCRTY_ASSGNMNT)
from .bird_data_model import RTNG_GRD_OTHR_ORGNSTN_ASSGNMNT
admin.site.register(RTNG_GRD_OTHR_ORGNSTN_ASSGNMNT)
from .bird_data_model import RTNG_SYSTM
admin.site.register(RTNG_SYSTM)
from .bird_data_model import RTNG_SYSTM_APPLD_LGL_PRSN
admin.site.register(RTNG_SYSTM_APPLD_LGL_PRSN)
from .bird_data_model import SBSDRY_JNT_VNTR_ASSCT_OTHR_ORGNSTN_ASSGNMNT
admin.site.register(SBSDRY_JNT_VNTR_ASSCT_OTHR_ORGNSTN_ASSGNMNT)
from .bird_data_model import SCRTY_ENTTY_RL_ASSGNMNT
admin.site.register(SCRTY_ENTTY_RL_ASSGNMNT)
from .bird_data_model import SCRTY_EXCHNG_TRDBL_DRVTV
admin.site.register(SCRTY_EXCHNG_TRDBL_DRVTV)
from .bird_data_model import SCRTY_HDGD_EXCHNG_TRDBL_DRVTV
admin.site.register(SCRTY_HDGD_EXCHNG_TRDBL_DRVTV)
from .bird_data_model import SCRTY_PSTN
admin.site.register(SCRTY_PSTN)
from .bird_data_model import SCRTY_PSTN_HDGD_OTC_DRVTV
admin.site.register(SCRTY_PSTN_HDGD_OTC_DRVTV)
from .bird_data_model import SCRTY_SCRTY_RPRCHS_AGRMNT_CMPNNT_ASSGNMNT
admin.site.register(SCRTY_SCRTY_RPRCHS_AGRMNT_CMPNNT_ASSGNMNT)
from .bird_data_model import SCTRY_BRRWNG_LNDNG_TRNSCTN_INCLDNG_CSH_CLLTRL
admin.site.register(SCTRY_BRRWNG_LNDNG_TRNSCTN_INCLDNG_CSH_CLLTRL)
from .bird_data_model import SHRT_SCRTY_PSTN_PRDNTL_PRTFL_ASSGNMNT
admin.site.register(SHRT_SCRTY_PSTN_PRDNTL_PRTFL_ASSGNMNT)
from .bird_data_model import SNTHTC_SCRTSTN
admin.site.register(SNTHTC_SCRTSTN)
from .bird_data_model import SYNDCTD_CNTRCT
admin.site.register(SYNDCTD_CNTRCT)
from .bird_data_model import TRDTNL_SCRTSTN
admin.site.register(TRDTNL_SCRTSTN)
from .bird_data_model import TRNCH_SYNTHTC_SCRTSTN_WTHT_SSPE_DPST
admin.site.register(TRNCH_SYNTHTC_SCRTSTN_WTHT_SSPE_DPST)
from .bird_data_model import TRNCH_SYNTHTC_SCRTSTN_WTHT_SSPE_FNNCL_GRNT
admin.site.register(TRNCH_SYNTHTC_SCRTSTN_WTHT_SSPE_FNNCL_GRNT)
from .bird_data_model import TRNCH_TRDTNL_SCRTSTN
admin.site.register(TRNCH_TRDTNL_SCRTSTN)
