-- dependent variable 'first_term_gpa' --
BEGIN
drop table if exists jkommera.dep_var;
create table jkommera.dep_var as
(
    select
        t1.person_uid,
        t2.id as puid,
        t1.profile_academic_period,
        t2.id || t1.profile_academic_period as pk1,
        t2.gpa as first_term_term_gpa,
        case when t2.gpa <= 2.0 then 1 when t2.gpa > 2.0 then 0 end as probation_ind,
        (CASE WHEN t2.gpa < 2.25 THEN 1 ELSE 0 END) as gpa_225_ind,
        (CASE WHEN t2.gpa < 2.5 THEN 1 ELSE 0 END) as gpa_25_ind,
        (CASE WHEN t2.gpa < 2.75 THEN 1 ELSE 0 END) as gpa_275_ind,
        (CASE WHEN t2.gpa < 3.0 THEN 1 ELSE 0 END) as gpa_3_ind,
        (CASE WHEN t2.gpa < 3.25 THEN 1 ELSE 0 END) as gpa_325_ind,
        (CASE WHEN t2.gpa < 3.5 THEN 1 ELSE 0 END) as gpa_35_ind,
        (CASE WHEN t2.gpa < 3.75 THEN 1 ELSE 0 END) as gpa_375_ind,
        (CASE WHEN t2.gpa < 4 THEN 1 ELSE 0 END) as gpa_4_ind
    from
        ods.student_profile_pu t1
    inner join 
        (
            select * from 
                ods.frz_gpa_by_term 
            where
                freeze_event = 'TERM_END' and
                academic_study_value = 'UG' and
                gpa_type = 'I'
        ) t2
    on 
        t1.person_uid = t2.person_uid and
        t1.profile_academic_period = t2.academic_period 
    where
        t1.profile_level = 'UG' and
        t1.profile_firstime_fulltime_ind = 'Y' and
        t1.profile_reporting_campus = 'PWL'
) distributed by (person_uid);

END;
