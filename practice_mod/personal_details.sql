-- function to create personla_details table --
 BEGIN


drop table if exists demos cascade;
create temp table demos as 
(
    select distinct
        person_uid,
        citizenship_ind,
        gender,
        reporting_ethnicity_code,
        1 as c
    from
        ods.person_detail_pu
) distributed by (person_uid);

drop table if exists citizen_temp;
perform madlib.pivot(
    --source
    'demos',
    --output
    'citizen_temp',
    --index
    'person_uid',
    --cols to transpose
    'citizenship_ind',
    --vals to operate on
    'c',
    --operation
    'SUM',
    --fill null with 0s
    '0'
    );

drop table if exists gender_temp;
perform madlib.pivot(
    --source
    'demos',
    --output
    'gender_temp',
    --index
    'person_uid',
    --cols to transpose
    'gender',
    --vals to operate on
    'c',
    --operation
    'SUM',
    --fill null with 0s
    '0'
    );

drop table if exists reporting_ethnicity_code_temp;
perform madlib.pivot(
    --source
    'demos',
    --output
    'reporting_ethnicity_code_temp',
    --index
    'person_uid',
    --cols to transpose
    'reporting_ethnicity_code',
    --vals to operate on
    'c',
    --operation
    'SUM',
    --fill null with 0s
    '0'
    );


drop table if exists jkommera.personal_details cascade;
create table jkommera.personal_details as 
(
    select
		t1.person_uid,
        "c_SUM_citizenship_ind_N" as citizenship_ind_N,
        "c_SUM_citizenship_ind_Y" as citizenship_ind_Y,
        "c_SUM_gender_M" as gender_M,
        "c_SUM_gender_F" as gender_F,
        "c_SUM_reporting_ethnicity_code_A" as race_amind,
        "c_SUM_reporting_ethnicity_code_B" as race_white,
        "c_SUM_reporting_ethnicity_code_C" as race_black,
        "c_SUM_reporting_ethnicity_code_D" as race_asian,
        "c_SUM_reporting_ethnicity_code_F" as race_nathaw,
        "c_SUM_reporting_ethnicity_code_I" as race_int,
        "c_SUM_reporting_ethnicity_code_X" as race_2more,
        "c_SUM_reporting_ethnicity_code_Y" as race_hisplat,
        "c_SUM_reporting_ethnicity_code_Z" as race_unk
    from
        citizen_temp t1
    left outer join
        gender_temp t2
    on t1.person_uid = t2.person_uid
    left outer join
        reporting_ethnicity_code_temp t3
    on t1.person_uid = t3.person_uid
) distributed by (person_uid);

END;
