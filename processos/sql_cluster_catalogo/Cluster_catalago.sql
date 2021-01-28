select  b.DS_MCR as DESCRICAO,
       b.cd_setmcr as SETOR,
       b.cd_espmcr   as    ESPECIE,
       b.ST_MCR_LOJ_VRT AS MARCACAO_B
from prdbat.mcr b
where b.cd_mcr in (select distinct cd_MSSMCR from prdbat.MCR_STM_SEC_MCR where DT_MSSMCR_INI_VIG >= '01.01.2018' and DT_MSSMCR_FIM_VIG IS NULL)
