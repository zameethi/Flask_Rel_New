select b.cd_mcr as SKU,
       b.DS_MCR as DESCRICAO,
       b.cd_setmcr as SETOR,
       b.cd_espmcr   as    ESPECIE,
       b.cd_clamcr 	as	CLASSE,
       b.ST_MCR_LOJ_VRT AS MARCACAO_B
from prdbat.mcr b
where b.cd_mcr in (select cd_MSSMCR as SKU from prdbat.MCR_STM_SEC_MCR where DT_MSSMCR_INI_VIG >= '01.11.2013' and DT_MSSMCR_FIM_VIG IS NULL)
and     b.ST_MCR_LOJ_VRT  = 'B'

