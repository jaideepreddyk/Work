
-- function for hs_core_gpa --

BEGIN

drop table if exists temp_high_school cascade;
create temp table temp_high_school as
(
    select person_uid, institution 
    from 
    (
        select 
            t1.person_uid,
            t1.institution,
            ROW_NUMBER() OVER(PARTITION BY t1.person_uid ORDER BY t1.institution DESC) AS rk
        from
        (
            select * from
            ods.previous_education
            where institution_type = 'H'
        )t1
        inner join
        (
            select person_uid, max(secondary_school_grad_date) as last_date
            from
            ods.previous_education
            where institution_type = 'H' 
            group by 1
        )t2
        on (t1.person_uid = t2.person_uid and 
            (t1.secondary_school_grad_date = t2.last_date or
            (t1.secondary_school_grad_date is null and t2.last_date is null)
            ))
        left outer join
            ods.institution t3
        on (t1.institution = t3.institution)
    )s
    where s.rk = 1
    group by 1,2
) distributed by (person_uid);

drop table if exists jkommera.hs_core_gpa cascade;
create table jkommera.hs_core_gpa as
(
	select 
		t1.person_uid, max(gpa::numeric) as hs_core_gpa
	from 
		temp_high_school t1,
		ods.secondary_school_subject t2
	where
		t1.person_uid = t2.person_uid and
		t1.institution = t2.institution and
		subject = 'CORE'
	group by 1
) distributed by (person_uid);

END;
