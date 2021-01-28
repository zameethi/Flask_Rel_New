SELECT MSSM.CD_SSMLOJ            AS "COD SORTIMENTO",
       MSSM.CD_SMLOJ             AS "COD SECAO",
       SMLO.DS_SMLOJ             AS "DESC SECAO",
       SSML.CD_TSLOJ             AS "COD TAMANHO",
       TSLO.DS_TSLOJ             AS "DESC TAMANHO",
       SSML.CD_TPLOJ             AS "COD PUBLICO",
       TPLO.DS_TPLOJ             AS "DESC PUBLICO",
       MSSM.CD_MSSMCR            AS "COD MERCADORIA",
       case when MSSM.CD_TIPPRD = 2
                then (select ds_amanc from prdbat.agp_mcr_anc where cd_amanc = MSSM.CD_MSSMCR)
                else MCR.DS_MCR
       end AS "DESC MERCADORIA",
       MSSM.CD_TIPPRD            AS "MERC / CONJ",
       MSSM.CD_MSSCMCR_PDD       AS "PRIORIDADE",
       MSSM.QT_MSSMCR            AS "QTDE",
       MSSM.ST_MSSMCR            AS "VOLTAGEM",
       MSSM.DT_MSSMCR_INI_VIG    AS "VIGENCIA INICIO",
       MSSM.DT_MSSMCR_FIM_VIG    AS "VIGENCIA FIM",
       MCR.CD_TSMCR              AS "STATUS",
       MCR.CD_SETMCR             AS "SETOR",
       case when MCR.CD_SETMCR <> 0
                then (select NM_setmcr from prdbat.set_mcr where cd_setmcr = MCR.CD_SETMCR)
                else 'NÃO ENCONTRADO'
       end AS "DESC SETOR",
       MCR.CD_CLAMCR             AS "CLASSE",
       case when MCR.CD_CLAMCR <> 0
                then (select DS_CLAMCR from prdbat.CLA_MCR where CD_CLAMCR = MCR.CD_CLAMCR)
                else 'NÃO ENCONTRADO'
       end AS "DESC CLASSE",
       MCR.CD_ESPMCR             AS "ESPECIE",
       case when MCR.CD_ESPMCR  <> 0
                then (SELECT B.NM_ESPMCR FROM PRDBAT.CLA_ESP_MCR A , PRDBAT.ESP_MCR  B
                      WHERE A.CD_SETMCR = MCR.CD_SETMCR
                      AND   A.CD_ESPMCR = MCR.CD_ESPMCR
                      AND   B.CD_ESPMCR = A.CD_ESPMCR
                      AND   A.CD_SETMCR = B.CD_SETMCR
                      fetch first 1 row only  )
                else 'NÃO ENCONTRADO'
       end AS "DESC ESPECIE",
       MCR.CD_MRCMCR             AS "MARCA",
       case when MCR.CD_MRCMCR  <> 0
                then (select NM_MRCMCR from prdbat.MRC_MCR where CD_MRCMCR = MCR.CD_MRCMCR)
                else 'NÃO ENCONTRADO'
       end AS "DESC MARCA"

  -- Mercadorias por sortimento(cluster)  e seção
  FROM PRDBAT.MCR_STM_SEC_MCR           MSSM
       -- Mercadoria
       -- LEFT JOIN PRDBAT.MCR             MCR  ON MSSM.CD_MSSMCR    = MCR.CD_MCR
	   LEFT JOIN PRDBAT.MCR             MCR  ON (MSSM.CD_MSSMCR    = MCR.CD_MCR and  mcr.st_mcr_svv =  'S'  and mssm. cd_tipprd = 1   )
       -- Seção da loja
       LEFT JOIN PRDBAT.SEC_MCR_LOJ     SMLO ON MSSM.CD_SMLOJ     = SMLO.CD_SMLOJ
       -- Sortimentos da seção (tamanho + publico)
       LEFT JOIN PRDBAT.STM_SEC_MCR_LOJ SSML ON (MSSM.CD_SSMLOJ   = SSML.CD_SSMLOJ AND MSSM.CD_SMLOJ       = SSML.CD_SMLOJ)
       -- tamanho seção loja
       LEFT JOIN PRDBAT.TAM_SEC_LOJ     TSLO ON (MSSM.CD_SMLOJ    = TSLO.CD_SMLOJ  AND SSML.CD_TSLOJ       = TSLO.CD_TSLOJ)
       -- Tipos de público
       LEFT JOIN PRDBAT.TIP_PUB_LOJ     TPLO ON TPLO.CD_TPLOJ     = SSML.CD_TPLOJ

 WHERE (MSSM.DT_MSSMCR_FIM_VIG >= CURRENT DATE OR MSSM.DT_MSSMCR_FIM_VIG IS NULL)
   AND (MSSM.DT_MSSMCR_INI_VIG <= CURRENT DATE)

-- LISTAR SEÇÕES DE MOVEIS parte 1
  AND MSSM.CD_SMLOJ    IN (8 ,   9,  11,  18,  19,  35,  40,  41,  42,  43,  46,  47,  53,  55,  56,  59,  60,  65,  85,  86,  88,  92, 94,  98)

WITH UR;