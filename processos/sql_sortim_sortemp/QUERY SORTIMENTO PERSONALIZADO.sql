SELECT DTB.CD_SMLOJ              AS "COD SECAO",
       SMLO.DS_SMLOJ             AS "DESC SECAO",
       DTB.CD_MSSMCR             as "COD PROD",
       MCR.DS_MCR                as "DESC PROD",
       DTB.CD_FIL                as "COD FIL",
       DTB.ST_DMSPER_OPR         AS "OPERACAO",
       DTB.QT_MSSMCR             AS "QUANT",
       DTB.DT_DMSPER_INI_VIG     AS "DT INICIO",
       DTB.CD_TIPPRD             AS "MERC/CONJ"

-- Mercadorias do Sortimento Personalizado por filial
from prdbat.DTB_MCR_STM_PER DTB

-- Seção de mercadoria da loja
LEFT JOIN PRDBAT.SEC_MCR_LOJ     SMLO
          ON DTB.CD_SMLOJ      = SMLO.CD_SMLOJ

-- Mercadoria
LEFT JOIN PRDBAT.MCR             MCR
          ON DTB.CD_MSSMCR     = MCR.CD_MCR

where  DT_DMSPER_FIM_VIG is null;