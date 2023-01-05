SELECT 
    C.ID AS CLAIM_ID,
    P.NAME_F || ' ' || P.NAME_L AS PAT_NAME,
    P.DOB,
    D.NAME AS DOCTOR_NAME,
    D.NPI,
    (SELECT MAX(SPECIALTY) FROM {{ source('dbt_demo','doc_specialties') }} DS WHERE DS.DOC_ID = D.ID) AS SPEC_NAME,
    PRIM_DIAG.ICD_10_CODE AS PRIMARY_DIAG,
    PRIM_DIAG.ICD_10_CODE_DESCRIP AS PRIMARY_DIAG_DESC,
    SEC_DIAG.ICD_10_CODE AS SEC_DIAG,
    SEC_DIAG.ICD_10_CODE_DESCRIP AS SEC_DIAG_DESC,
    C.CLAIMNUMBER,
    CA.TOTAL_CLAIM_AMNT,
    CA.PAID_AMNT
FROM 
    {{ source('dbt_demo','patients') }} P
    JOIN {{ source('dbt_demo','claims') }} C
        ON P.ID = C.PAT_ID
    LEFT JOIN (
        SELECT 
            CL.CLAIM_ID, 
            SUM(CHRGAMNT) TOTAL_CLAIM_AMNT,
            SUM(PAID_AMNT) PAID_AMNT
        FROM {{ source('dbt_demo','claim_line') }} CL
        WHERE STATUS IN ('billed', 'adjudicated', 'closed')
        GROUP BY 1
    ) CA 
        ON C.ID = CA.CLAIM_ID
    LEFT JOIN (
        SELECT 
            CLAIM_ID,
            ICD10 AS ICD_10_CODE,
            ICD10DESC AS ICD_10_CODE_DESCRIP,
            SPLIT_PART(ICD10, '.', 1) AS PARENT_ICD10
        FROM {{ source('dbt_demo','claim_diagnosis') }}
        WHERE POS = 1
    ) AS PRIM_DIAG
    ON C.ID = PRIM_DIAG.CLAIM_ID
    LEFT JOIN (
        SELECT 
            CLAIM_ID,
            ICD10 AS ICD_10_CODE,
            ICD10DESC AS ICD_10_CODE_DESCRIP,
            SPLIT_PART(ICD10, '.', 1) AS PARENT_ICD10
        FROM {{ source('dbt_demo','claim_diagnosis') }}
        WHERE POS = 2
    ) AS SEC_DIAG
    ON C.ID = SEC_DIAG.CLAIM_ID
    LEFT JOIN {{ source('dbt_demo','doctors') }} D
        ON C.DOC_ID = D.ID 

WHERE C.TEST = FALSE 
AND P.EMAIL NOT LIKE '%@test-patient.com'
AND C.BILL_ATTMPS > 0