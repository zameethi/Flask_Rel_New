SELECT distinct
        A.CD_EMPGCB       AS  "EMPRESA",
        A.CD_FIL          AS  "FILIAL",
        case when A.CD_FIL <> 0
             Then (SELECT '220'
                        FROM PRDBAT.AGP_VRS_FIL_MCR   T1, PRDBAT.FIL   T2
                        WHERE   T1.CD_TAFMCR = 13
                        AND     T1.CD_MFAGP  = 1
                        AND     T1.CD_EMPGCB = 21
                        AND     T1.CD_EMPGCB = T2.CD_EMPGCB
                        AND     T1.CD_FIL    = T2.CD_FIL
                        AND     T1.CD_FIL    = A.CD_FIL
                        AND     T2.CD_FIL    = A.CD_FIL  )
              end AS "VOLTAGEM",

        D.CD_TPLOJ        AS  "COD PUBLICO",
        E.DS_TPLOJ        AS  "DESC PUBLICO",
        A.CD_SMLOJ        AS  "COD SECAO",
        G.DS_SMLOJ        AS  "DESC SECAO",
        B.CD_SSMLOJ       AS  "COD SORTIMENTO",
        A.CD_TSLOJ        AS  "COD TAMANHO" ,
        F.DS_TSLOJ        AS  "DESC TAMANHO " ,
        E.DS_TPLOJ        AS  "TIPO PUBLICO",
        A.ST_TSFIL        AS  "SOLIC.VOLT DIF"

FROM PRDBAT.TAM_SEC_FIL     A ,
     PRDBAT.STM_SEC_MCR_LOJ B ,
     PRDBAT.TIP_PUB_LOJ_FIL D ,
     PRDBAT.TIP_PUB_LOJ     E ,
     PRDBAT.TAM_SEC_LOJ     F ,
     PRDBAT.SEC_MCR_LOJ     G

  WHERE A.CD_EMPGCB = 21
    AND A.CD_SMLOJ            = B.CD_SMLOJ AND A.CD_TSLOJ  = B.CD_TSLOJ
    AND A.DT_TSFIL_INI_VIG   <= CURRENT DATE
    AND (A.DT_TSFIL_FIM_VIG  >= CURRENT DATE OR A.DT_TSFIL_FIM_VIG IS NULL)
    AND  D.CD_EMPGCB         = 21
    AND  D.CD_FIL            = A.CD_FIL
    AND B.CD_TPLOJ           = D.CD_TPLOJ
    AND  D.DT_TPLFIL_INI_VIG <= CURRENT DATE
    AND (D.DT_TPLFIL_FIM_VIG >= CURRENT DATE OR D.DT_TPLFIL_FIM_VIG IS NULL)
    AND E.CD_TPLOJ = D.CD_TPLOJ
    AND (F.CD_SMLOJ = A.CD_SMLOJ AND F.CD_TSLOJ = A.CD_TSLOJ )
    AND G.CD_SMLOJ = A.CD_SMLOJ

UNION

SELECT distinct
        A.CD_EMPGCB       AS  "EMPRESA",
        A.CD_FIL          AS  "FILIAL",
        case when A.CD_FIL <> 0
             Then (SELECT '220'
                        FROM PRDBAT.AGP_VRS_FIL_MCR   T1, PRDBAT.FIL   T2
                        WHERE   T1.CD_TAFMCR = 13
                        AND     T1.CD_MFAGP  = 1
                        AND     T1.CD_EMPGCB = 21
                        AND     T1.CD_EMPGCB = T2.CD_EMPGCB
                        AND     T1.CD_FIL    = T2.CD_FIL
                        AND     T1.CD_FIL    = A.CD_FIL
                        AND     T2.CD_FIL    = A.CD_FIL  )
              end AS "VOLTAGEM",
        0                 AS  "COD PUBLICO",
        'sem publico'     AS  "DESC PUBLICO",
        A.CD_SMLOJ        AS  "COD SECAO",
        G.DS_SMLOJ        AS  "DESC SECAO",
        B.CD_SSMLOJ       AS  "COD SORTIMENTO",
        A.CD_TSLOJ        AS  "COD TAMANHO" ,
        F.DS_TSLOJ        AS  "DESC TAMANHO " ,
        'sem publico'     AS  "TIPO PUBLICO",
        A.ST_TSFIL        AS  "SOLIC.VOLT DIF"

FROM PRDBAT.TAM_SEC_FIL     A ,
     PRDBAT.STM_SEC_MCR_LOJ B ,
     PRDBAT.TAM_SEC_LOJ     F ,
     PRDBAT.SEC_MCR_LOJ     G
  WHERE A.CD_EMPGCB = 21
    AND A.CD_SMLOJ           = B.CD_SMLOJ AND A.CD_TSLOJ  = B.CD_TSLOJ
    AND A.DT_TSFIL_INI_VIG   <= CURRENT DATE
    AND (A.DT_TSFIL_FIM_VIG  >= CURRENT DATE OR A.DT_TSFIL_FIM_VIG IS NULL)
    and b.cd_tploj is null
    AND (F.CD_SMLOJ = A.CD_SMLOJ AND F.CD_TSLOJ = A.CD_TSLOJ )
    AND G.CD_SMLOJ = A.CD_SMLOJ;