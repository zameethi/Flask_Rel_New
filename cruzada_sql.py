sql = '''SELECT        DISTINCT
			  T1.CD_EMPGCB         as "EMPRESA"
			 ,T1.CD_SMLOJ          as "SECAO"
			 ,T1.CD_FIL            as "FILIAL"
			 ,T2.CD_SSMLOJ         as "SORT"
			 ,T2.CD_TSLOJ          as "TAMANHO"
			 ,T6.DS_TSLOJ          as "DESC TAMANHO"
			 ,T2.CD_TPLOJ          as "COD PUBLICO"
			 ,T5.DS_TPLOJ          as "DESC PUBLICO"
			 ,T3.CD_MSSMCR         as "COD MERC/CONJ"
			 ,T3.CD_TIPPRD         as "MERC/CONJ"
			 ,case when T3.CD_TIPPRD = 2 
				  then (select ds_amanc from prdbat.agp_mcr_anc where cd_amanc = T3.CD_MSSMCR) 
				  else T7.DS_MCR
				   end AS "DESC MERC/CONJ"
			 ,t7.CD_TSMCR          AS "STATUS"      
			 ,T3.DT_MSSMCR_INI_VIG as "INI VIG"
			 ,T3.CD_MSSCMCR_PDD    as "PRIORIDADE"
			 ,T3.CD_MSSMCR_ORD_PDD as "ORDEM"
			 ,T3.QT_MSSMCR         as "QTD"
			 ,T3.ST_MSSMCR         as "VOLTAGEM"
			 ,T1.ST_TSFIL          AS "FILIAL BIVOLT"
	  

  FROM        prdbat.TAM_SEC_FIL       T1
			 ,prdbat.STM_SEC_MCR_LOJ   T2
			 ,prdbat.MCR_STM_SEC_MCR   T3
			 ,prdbat.TIP_PUB_LOJ_FIL   T4
			 ,prdbat.TIP_PUB_LOJ       T5
			 ,prdbat.TAM_SEC_LOJ       T6
			 ,prdbat.MCR               T7
 WHERE        T1.CD_EMPGCB           =   21     
   AND        T1.CD_FIL              >   0
   AND        T1.DT_TSFIL_INI_VIG   <=   current date
   AND       (T1.DT_TSFIL_FIM_VIG   IS    NULL      
	OR        T1.DT_TSFIL_FIM_VIG   >=   current date)                   
   AND        T2.CD_SMLOJ            =    T1.CD_SMLOJ
   AND        T2.CD_TSLOJ            =    T1.CD_TSLOJ
   AND        T3.CD_SSMLOJ           =    T2.CD_SSMLOJ
   AND        T3.CD_SMLOJ            =    T2.CD_SMLOJ     
   AND        T3.DT_MSSMCR_INI_VIG  <=   current date 
   AND        T3.DT_MSSMCR_FIM_VIG   IS NULL  
   AND        T4.CD_EMPGCB           =    T1.CD_EMPGCB
   AND        T4.CD_FIL              =    T1.CD_FIL
   AND        T4.CD_TPLOJ            =    T2.CD_TPLOJ
   AND       (T4.DT_TPLFIL_FIM_VIG  IS    NULL
	OR        T4.DT_TPLFIL_FIM_VIG  >=   current date)
   AND        T5.CD_TPLOJ            =    T4.CD_TPLOJ
   AND        T6.CD_SMLOJ            =    T1.CD_SMLOJ
   AND        T6.CD_TSLOJ            =    T1.CD_TSLOJ
   -- não extrair base cruzada para móveis
/*               AND t3.CD_SMLOJ   not     IN (8 ,   9,  11,  18,  19,  35,  40,  41,  42,  43,  46,  47,  53,  55,  56,  59,  60,  65,  85,  86,  88,  92,
								 94,  98, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116, 117, 118, 119, 120, 
								121, 122, 124)   -- SECAO */
   AND t3.cd_mssmcr = t7.cd_mcr
   
   AND t3.CD_SMLOJ in ({})     -- SEÇÃO                  
--              and T1.CD_FIL  = 1000             
--               and T3.CD_MSSMCR in  (3010147,3010155)  
--              and   T1.CD_FIL  in (61,1057)
   
 UNION

SELECT        DISTINCT
			  T1.CD_EMPGCB    as "EMPRESA"
			 ,T1.CD_SMLOJ     as "SECAO"
			 ,T1.CD_FIL       as "FILIAL"
			 ,T2.CD_SSMLOJ    as "SORT"
			 ,T2.CD_TSLOJ     as "TAMANHO"
			 ,T4.DS_TSLOJ     as "DESC TAMANHO"
			 ,T2.CD_TPLOJ     as "COD PUBLICO"
			 ,' '             as "DESC PUBLICO"
			 ,T3.CD_MSSMCR    as "COD MERC/CONJ"
			 ,T3.CD_TIPPRD    as "MERC/CONJ"
			 ,case when T3.CD_TIPPRD = 2 
				  then (select ds_amanc from prdbat.agp_mcr_anc where cd_amanc = T3.CD_MSSMCR) 
			 else T5.DS_MCR
			 end AS "DESC MERC/CONJ"
			 ,t5.CD_TSMCR          AS "STATUS"
			 ,T3.DT_MSSMCR_INI_VIG as "INI VIG"
			 ,T3.CD_MSSCMCR_PDD    as "PRIORIDADE"
			 ,T3.CD_MSSMCR_ORD_PDD as "ORDEM"
			 ,T3.QT_MSSMCR         as "QTD"
			 ,T3.ST_MSSMCR         as "VOLTAGEM"
			 ,T1.ST_TSFIL          AS "FILIAL BIVOLT" 
						  
  FROM        prdbat.TAM_SEC_FIL       T1
			 ,prdbat.STM_SEC_MCR_LOJ   T2
			 ,prdbat.MCR_STM_SEC_MCR   T3
			 ,prdbat.TAM_SEC_LOJ       T4
			 ,prdbat.MCR               T5
 WHERE        T1.CD_EMPGCB           =   21     
   AND       (T1.DT_TSFIL_FIM_VIG   IS    NULL
	OR        T1.DT_TSFIL_FIM_VIG    >   current date)
   AND        T2.CD_SMLOJ            =    T1.CD_SMLOJ
   AND        T2.CD_TSLOJ            =    T1.CD_TSLOJ
   AND        T2.CD_TPLOJ           IS    NULL
   AND        T3.CD_SSMLOJ           =    T2.CD_SSMLOJ
   AND        T3.CD_SMLOJ            =    T2.CD_SMLOJ
   AND        T3.DT_MSSMCR_INI_VIG  <=   current date
   AND        T3.DT_MSSMCR_FIM_VIG   IS  NULL
   AND        T2.CD_TPLOJ           IS    NULL
   AND        T4.CD_SMLOJ            =    T1.CD_SMLOJ
   AND        T4.CD_TSLOJ            =    T1.CD_TSLOJ

   -- não extrair base cruzada para móveis

/*               AND t3.CD_SMLOJ   not     IN (8 ,   9,  11,  18,  19,  35,  40,  41,  42,  43,  46,  47,  53,  55,  56,  59,  60,  65,  85,  86,  88,  92,
								 94,  98, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116, 117, 118, 119, 120, 
							   121, 122, 124)   -- SECAO */
								
   AND t3.CD_SMLOJ in ({})     -- SEÇÃO       
--              and T1.CD_FIL  = 1000   
--                and T3.CD_MSSMCR in  (3010147,3010155)   
--               and   T1.CD_FIL  in (61,1057)
						
   AND t3.cd_mssmcr = t5.cd_mcr ;'''