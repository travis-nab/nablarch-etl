SELECT_PROJECT =
SELECT
    PROJECT_ID,
    PROJECT_NAME
FROM
    PROJECT
WHERE
    $if(customerId)     {CUSTOMER_ID = :customerId}
    AND $if(projectName) {PROJECT_NAME LIKE  :%projectName%}
$sort(sortId){
    (idAsc PROJECT_ID)
    (idDesc PROJECT_ID DESC)
    (nameAsc PROJECT_NAME, PROJECT_ID)
    (nameDesc PROJECT_NAME DESC, PROJECT_ID DESC)
}

SELECT_PET =
SELECT
    PET_ID,
    PET_NAME
FROM
    PET
WHERE
    $if(ownerId)     {OWNER_ID = :ownerId}
    AND $if(petName) {PET_NAME LIKE  :%petName%}
$sort(sortId){
    (idAsc PET_ID)
    (idDesc PET_ID DESC)
    (nameAsc PET_NAME, PROJECT_ID)
    (nameDesc PET_NAME DESC, PROJECT_ID DESC)
}
