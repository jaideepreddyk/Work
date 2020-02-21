 -- function to create hs_inst_gpa --



 BEGIN

--find the most recent highschool and HSGPA for each student
--this way we only pick one HS per person
drop table if exists most_recent_highschool;
create temp table most_recent_highschool
as
(
	select * from
	(
		select 
			prev_edu.institution, 
			prev_edu.person_uid,
			inst.nation as hs_nation,
			school_gpa::numeric as hs_gpa,
			profile_academic_period,
			row_number() over (partition by prev_edu.person_uid order by secondary_school_grad_date desc) as rn 
		from
			ods.previous_education prev_edu
		inner join
			(select
				person_uid, profile_academic_period
			from
				ods.student_profile_pu 
			where
				profile_level = 'UG' and
				profile_reporting_campus = 'PWL' and
				profile_admissions_population in ('B','SB') and
				substr(profile_academic_period,5,7) != ('30') and
				profile_firstime_fulltime_ind = 'Y'
			) profile
		on (profile.person_uid = prev_edu.person_uid)
		left outer join
			ods.institution inst
		on prev_edu.institution = inst.institution
		where 
			prev_edu.institution != 'UNKHSC' and 
			prev_edu.institution != 'UNKHS1' and 
			prev_edu.institution != 'UNKHS2' and
			prev_edu.institution != 'UNKHS3' and
			prev_edu.institution_type = 'H'
	)q
	where rn = 1
) distributed by (person_uid);

--select the first semester term GPA for each student along with their institution
--only use people who were first time full time freshmen at one time	
drop table if exists students_for_calc;
create temp table students_for_calc
as
(
	select 
		first_gpas.person_uid, 
		prev_inst.institution,
		cluster,
		hs_nation,
		profile_academic_period,
		gpa as first_term_gpa
	from
	(
		select
			person_uid,
			gpa,
			academic_period,
			row_number() over (partition by person_uid order by academic_period) as nr
		from 
			ods.frz_gpa_by_term
		where
			gpa_type = 'I'
			and freeze_event = 'TERM_END'
			and substr(academic_period,5,7) != ('30')
	) first_gpas
	inner join
	(
		--most recently graduated high school
		select 
			institution, 
			hs_nation,
			person_uid 
		from most_recent_highschool
	) prev_inst
	on (prev_inst.person_uid = first_gpas.person_uid)
	--first time full time students. grabbing distinct IDs and inner joining
	--so that if they were EVER first time full time, they keep in our list
	inner join
	(
		select 
			person_uid, min(profile_academic_period) as profile_academic_period
		from
			ods.student_profile_pu 
		where
			profile_level = 'UG' and
			profile_reporting_campus = 'PWL' and
			profile_admissions_population in ('B','SB') and
			substr(profile_academic_period,5,7) != ('30') and
			profile_firstime_fulltime_ind = 'Y'
		group by 1
	) firsttime_fulltime
	on (firsttime_fulltime.person_uid = first_gpas.person_uid)
		and first_gpas.academic_period = firsttime_fulltime.profile_academic_period
	left outer join
		adhoc.high_school_clusters as hs_clusts
	on prev_inst.institution = hs_clusts.institution
) distributed by (person_uid);

--calculate the institutional GPAs of all high schools that supplied at least 25 new beginners in the past to purdue
drop table if exists large_institution_inst_gpas;
create temp table large_institution_inst_gpas
as
(
	--pull all high schools where we have had at least 25 students at purdue who had been FTFT students
	--calculate the first year GPA of students from those high schools, join on
	--this will have the effect of keeping every GPA from every student inside of every year that comes after it
	--while keeping the academic period the same across for joining
	select 
		profile_academic_period, 
		institution, 
		number_of_students, 
		hs_inst_gpa
	from 
		(
		select 
			t1.profile_academic_period, 
			t1.institution, 
			count(distinct t2.person_uid) as number_of_students, 
			avg(t2.first_term_gpa) as hs_inst_gpa
		from
			students_for_calc t1
		left outer join
			students_for_calc t2
		on 
			t1.institution = t2.institution 
			--make sure to only use PRIOR gpas
			and t1.profile_academic_period > t2.profile_academic_period
		group by t1.profile_academic_period, t1.institution
		) inst_counts
	where 
		number_of_students > 25
) distributed randomly;

--calculate the institutional GPAs of all high school clusters
--utilize everyone from that cluster, even ones from high schools 
--too small to measure independently
drop table if exists cluster_inst_gpa;
create temp table cluster_inst_gpa
as
(
	select 
		profile_academic_period, 
		cluster, 
		number_of_students, 
		cluster_inst_gpa
	from 
		(
		select 
			t1.profile_academic_period, 
			t1.cluster, 
			count(distinct t2.person_uid) as number_of_students, 
			avg(t2.first_term_gpa) as cluster_inst_gpa
		from
			students_for_calc t1
		left outer join
			students_for_calc t2
		on 
			t1.cluster = t2.cluster 
			--make sure to only use PRIOR gpas
			and t1.profile_academic_period > t2.profile_academic_period
		group by t1.profile_academic_period, t1.cluster
		) cluster_counts
	where 
		number_of_students > 25
) distributed randomly;

--calculate the institutional GPAs of all high school nations
--utilize everyone from that cluster, even ones from high schools 
--too small to measure independently
drop table if exists nation_inst_gpa;
create temp table nation_inst_gpa
as
(
	select 
		profile_academic_period, 
		hs_nation, 
		number_of_students, 
		nation_inst_gpa
	from 
		(
		select 
			t1.profile_academic_period, 
			t1.hs_nation, 
			count(distinct t2.person_uid) as number_of_students, 
			avg(t2.first_term_gpa) as nation_inst_gpa
		from
			students_for_calc t1
		left outer join
			students_for_calc t2
		on 
			t1.hs_nation = t2.hs_nation 
			--make sure to only use PRIOR gpas
			and t1.profile_academic_period > t2.profile_academic_period
		group by t1.profile_academic_period, t1.hs_nation
		) cluster_counts
	where 
		number_of_students > 25
) distributed randomly;

drop table if exists jkommera.hs_inst_gpa;
create table jkommera.hs_inst_gpa
as
(
	--alright, this looks insane but bear with me
	--we want to have every institution in every semester if it has the history for it
	--however, during a lot of semesters we don't get any students entering from HS's
	--even if that HS had 25+ students in the past. This happens in most springs, and keeps the HS
	--from appearing in the prior table at all in that semester. 
	--What I'm doing here is using the most recent previous HS Inst GPA 
	--whenever we have a semester that does not have that institution at all due to zero incoming students from it
	
	--all institutions that ever had a inst_gpa, with every semester listed out for each
	WITH verbose_inst_period_list as
	(
		select distinct academic_period, institution from
			(select distinct academic_period from oir.academic_calendar)q1
		left outer join
			(select distinct institution from ods.institution)q2
		on TRUE
	),
	most_recent_large_institution_inst_gpa as 
	(		
		select * from
		(
			select 
				t1.institution,
				t1.academic_period,
				t2.hs_inst_gpa,
				t2.number_of_students,
				row_number() over (partition by t1.institution,t1.academic_period order by t2.profile_academic_period desc) as rn
			from
				verbose_inst_period_list t1
			left outer join
				large_institution_inst_gpas t2
			on t1.institution=t2.institution and t1.academic_period >= t2.profile_academic_period
		) q
		where rn = 1 and hs_inst_gpa is not null
	),
	most_recent_cluster_inst_gpa as 
	(		
		select * from
		(
			select 
				t1.institution,
				t1.academic_period,
				t2.cluster_inst_gpa,
				row_number() over (partition by t1.institution,t1.academic_period order by t2.profile_academic_period desc) as rn
			from
				verbose_inst_period_list t1
			left outer join
				(
					select 
						institution, 
						profile_academic_period,
						cluster_inst_gpa
					from
						(select distinct institution, cluster from adhoc.high_school_clusters) q1
					inner join 
						cluster_inst_gpa q2
					on (q1.cluster = q2.cluster)
				) t2
			on t1.institution=t2.institution and t1.academic_period >= t2.profile_academic_period
		) q
		where rn = 1 and cluster_inst_gpa is not null
	),
	most_recent_nation_inst_gpa as 
	(		
		select * from
		(
			select 
				t1.institution,
				t1.academic_period,
				t2.nation_inst_gpa,
				row_number() over (partition by t1.institution,t1.academic_period order by t2.profile_academic_period desc) as rn
			from
				verbose_inst_period_list t1
			left outer join
				(
					select 
						institution, 
						profile_academic_period,
						nation_inst_gpa
					from
						(select distinct institution, nation from ods.institution) q1
					inner join 
						nation_inst_gpa q2
					on (q1.nation = q2.hs_nation)
				) t2
			on t1.institution=t2.institution and t1.academic_period >= t2.profile_academic_period
		) q
		where rn = 1 and nation_inst_gpa is not null
	)
	select 
		person_hs.person_uid, 
		person_hs.profile_academic_period,
		person_hs.institution,
		coalesce(hs_inst_gpa, cluster_inst_gpa, nation_inst_gpa) as hs_inst_gpa,
		most_recent_large_institution_inst_gpa.number_of_students as num_students_for_inst_gpa,
		(hs_gpa - coalesce(hs_inst_gpa, cluster_inst_gpa, nation_inst_gpa)) as hs_gpa_vs_hs_inst_gpa_diff
	from
	(
	--pull everyone's HS, just the most recently graduated one, to serve as our base table
		select 
			person_uid, 
			institution,
			profile_academic_period,
			hs_gpa
		from
			most_recent_highschool
	) person_hs
	--join on the inst gpas
	left outer join
		most_recent_large_institution_inst_gpa
	on (most_recent_large_institution_inst_gpa.institution = person_hs.institution
		and most_recent_large_institution_inst_gpa.academic_period = person_hs.profile_academic_period)
	left outer join
		most_recent_cluster_inst_gpa
	on (most_recent_cluster_inst_gpa.institution = person_hs.institution
		and most_recent_cluster_inst_gpa.academic_period = person_hs.profile_academic_period)
	left outer join
		most_recent_nation_inst_gpa
	on (most_recent_nation_inst_gpa.institution = person_hs.institution
		and most_recent_nation_inst_gpa.academic_period = person_hs.profile_academic_period)
) distributed by (person_uid);

END;
