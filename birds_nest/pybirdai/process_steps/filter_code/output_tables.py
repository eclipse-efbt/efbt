from pybirdai.process_steps.pybird.orchestration import Orchestration
from datetime import datetime
from pybirdai.annotations.decorators import lineage
from .F_01_01_REF_FINREP_3_0_logic import *

class F_01_01_REF_FINREP_3_0:
	unionOfLayers = None #  F_01_01_REF_FINREP_3_0_UnionItem  unionOfLayers
	@lineage(dependencies={"unionOfLayers.CRRYNG_AMNT"})
	def CRRYNG_AMNT(self) -> int:
		return self.unionOfLayers.CRRYNG_AMNT()

	@lineage(dependencies={"unionOfLayers.ACCNTNG_CLSSFCTN"})
	def ACCNTNG_CLSSFCTN(self) -> str:
		''' return string from ACCNTNG_CLSSFCTN enumeration '''
		return self.unionOfLayers.ACCNTNG_CLSSFCTN()

	@lineage(dependencies={"unionOfLayers.TYP_INSTRMNT"})
	def TYP_INSTRMNT(self) -> str:
		''' return string from TYP_INSTRMNT enumeration '''
		return self.unionOfLayers.TYP_INSTRMNT()

	@lineage(dependencies={"unionOfLayers.HLD_SL_INDCTR"})
	def HLD_SL_INDCTR(self) -> str:
		''' return string from HLD_SL_INDCTR enumeration '''
		return self.unionOfLayers.HLD_SL_INDCTR()

	@lineage(dependencies={"unionOfLayers.RPYMNT_RGHTS"})
	def RPYMNT_RGHTS(self) -> str:
		''' return string from RPYMNT_RGHTS enumeration '''
		return self.unionOfLayers.RPYMNT_RGHTS()

	@lineage(dependencies={"unionOfLayers.INSTTTNL_SCTR"})
	def INSTTTNL_SCTR(self) -> str:
		''' return string from INSTTTNL_SCTR enumeration '''
		return self.unionOfLayers.INSTTTNL_SCTR()

	@lineage(dependencies={"unionOfLayers.PRTY_RL_TYP"})
	def PRTY_RL_TYP(self) -> str:
		''' return string from ENTTY_RL_TYP enumeration '''
		return self.unionOfLayers.PRTY_RL_TYP()

	@lineage(dependencies={"unionOfLayers.MN_DBTR_INDCTR"})
	def MN_DBTR_INDCTR(self) -> str:
		''' return string from MN_DBTR_INDCTR enumeration '''
		return self.unionOfLayers.MN_DBTR_INDCTR()

	@lineage(dependencies={"unionOfLayers.NGTBL_SCRTY_INDCTR"})
	def NGTBL_SCRTY_INDCTR(self) -> str:
		''' return string from NGTBL_SCRTY enumeration '''
		return self.unionOfLayers.NGTBL_SCRTY_INDCTR()

	@lineage(dependencies={"unionOfLayers.SCRTY_EXCHNG_TRDBL_DRVTV_TYP"})
	def SCRTY_EXCHNG_TRDBL_DRVTV_TYP(self) -> str:
		''' return string from SCRTY_EXCHNG_TRDBL_DRVTV_TYP enumeration '''
		return self.unionOfLayers.SCRTY_EXCHNG_TRDBL_DRVTV_TYP()

	@lineage(dependencies={"unionOfLayers.TYP_ACCNTNG_ITM"})
	def TYP_ACCNTNG_ITM(self) -> str:
		''' return string from TYP_ACCNTNG_ITM enumeration '''
		return self.unionOfLayers.TYP_ACCNTNG_ITM()

	@lineage(dependencies={"unionOfLayers.TYP_HDG"})
	def TYP_HDG(self) -> str:
		''' return string from TYP_HDG enumeration '''
		return self.unionOfLayers.TYP_HDG()


class F_01_01_REF_FINREP_3_0_Table :
	F_01_01_REF_FINREP_3_0_UnionTable = None # unionOfLayersTable
	F_01_01_REF_FINREP_3_0s = [] #F_01_01_REF_FINREP_3_0[]
	def  calc_F_01_01_REF_FINREP_3_0s(self) -> list[F_01_01_REF_FINREP_3_0] :
		items = [] # F_01_01_REF_FINREP_3_0[]
		for item in self.F_01_01_REF_FINREP_3_0_UnionTable.F_01_01_REF_FINREP_3_0_UnionItems:
			newItem = F_01_01_REF_FINREP_3_0()
			newItem.unionOfLayers = item
			items.append(newItem)
		return items
	def init(self):
		Orchestration().init(self)
		self.F_01_01_REF_FINREP_3_0s = []
		self.F_01_01_REF_FINREP_3_0s.extend(self.calc_F_01_01_REF_FINREP_3_0s())
		CSVConverter.persist_object_as_csv(self,True)
		return None

from .F_05_01_REF_FINREP_3_0_logic import *

class F_05_01_REF_FINREP_3_0:
	unionOfLayers = None #  F_05_01_REF_FINREP_3_0_UnionItem  unionOfLayers
	@lineage(dependencies={"unionOfLayers.GRSS_CRRYNG_AMNT"})
	def GRSS_CRRYNG_AMNT(self) -> int:
		return self.unionOfLayers.GRSS_CRRYNG_AMNT()

	@lineage(dependencies={"unionOfLayers.PRPS"})
	def PRPS(self) -> str:
		''' return string from PRPS enumeration '''
		return self.unionOfLayers.PRPS()

	@lineage(dependencies={"unionOfLayers.ACCNTNG_CLSSFCTN"})
	def ACCNTNG_CLSSFCTN(self) -> str:
		''' return string from ACCNTNG_CLSSFCTN enumeration '''
		return self.unionOfLayers.ACCNTNG_CLSSFCTN()

	@lineage(dependencies={"unionOfLayers.HLD_SL_INDCTR"})
	def HLD_SL_INDCTR(self) -> str:
		''' return string from HLD_SL_INDCTR enumeration '''
		return self.unionOfLayers.HLD_SL_INDCTR()

	@lineage(dependencies={"unionOfLayers.INSTTTNL_SCTR"})
	def INSTTTNL_SCTR(self) -> str:
		''' return string from INSTTTNL_SCTR enumeration '''
		return self.unionOfLayers.INSTTTNL_SCTR()

	@lineage(dependencies={"unionOfLayers.PRTY_RL_TYP"})
	def PRTY_RL_TYP(self) -> str:
		''' return string from ENTTY_RL_TYP enumeration '''
		return self.unionOfLayers.PRTY_RL_TYP()

	@lineage(dependencies={"unionOfLayers.MN_DBTR_INDCTR"})
	def MN_DBTR_INDCTR(self) -> str:
		''' return string from MN_DBTR_INDCTR enumeration '''
		return self.unionOfLayers.MN_DBTR_INDCTR()

	@lineage(dependencies={"unionOfLayers.TYP_INSTRMNT"})
	def TYP_INSTRMNT(self) -> str:
		''' return string from TYP_INSTRMNT enumeration '''
		return self.unionOfLayers.TYP_INSTRMNT()

	@lineage(dependencies={"unionOfLayers.RPYMNT_RGHTS"})
	def RPYMNT_RGHTS(self) -> str:
		''' return string from RPYMNT_RGHTS enumeration '''
		return self.unionOfLayers.RPYMNT_RGHTS()

	@lineage(dependencies={"unionOfLayers.PRJCT_FNNC_LN"})
	def PRJCT_FNNC_LN(self) -> str:
		''' return string from PRJCT_FNNC_LN enumeration '''
		return self.unionOfLayers.PRJCT_FNNC_LN()

	@lineage(dependencies={"unionOfLayers.NGTBL_SCRTY_INDCTR"})
	def NGTBL_SCRTY_INDCTR(self) -> str:
		''' return string from NGTBL_SCRTY enumeration '''
		return self.unionOfLayers.NGTBL_SCRTY_INDCTR()

	@lineage(dependencies={"unionOfLayers.CRRYNG_AMNT"})
	def CRRYNG_AMNT(self) -> int:
		return self.unionOfLayers.CRRYNG_AMNT()

	@lineage(dependencies={"unionOfLayers.MLTLTRL_DVLPMNT_BNK_INDCTR"})
	def MLTLTRL_DVLPMNT_BNK_INDCTR(self) -> str:
		''' return string from MLTLTRL_DVLPMNT_BNK_INDCTR enumeration '''
		return self.unionOfLayers.MLTLTRL_DVLPMNT_BNK_INDCTR()


class F_05_01_REF_FINREP_3_0_Table :
	F_05_01_REF_FINREP_3_0_UnionTable = None # unionOfLayersTable
	F_05_01_REF_FINREP_3_0s = [] #F_05_01_REF_FINREP_3_0[]
	def  calc_F_05_01_REF_FINREP_3_0s(self) -> list[F_05_01_REF_FINREP_3_0] :
		items = [] # F_05_01_REF_FINREP_3_0[]
		for item in self.F_05_01_REF_FINREP_3_0_UnionTable.F_05_01_REF_FINREP_3_0_UnionItems:
			newItem = F_05_01_REF_FINREP_3_0()
			newItem.unionOfLayers = item
			items.append(newItem)
		return items
	def init(self):
		Orchestration().init(self)
		self.F_05_01_REF_FINREP_3_0s = []
		self.F_05_01_REF_FINREP_3_0s.extend(self.calc_F_05_01_REF_FINREP_3_0s())
		CSVConverter.persist_object_as_csv(self,True)
		return None
