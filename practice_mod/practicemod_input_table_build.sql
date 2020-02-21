--- this functions builds the input table for my prject using the tutuorial template ---

create or replace function jkommera.practice_mod_input_table_build(feat_sources text[], output_schema text, daily_table_waited_days int) returns void as
$BODY$

DECLARE 
model_name text;
last_update_date date;
last_update_term text;
current_date_str text;
current_term text;
daily_refresh_tables text[];
census_refresh_tables text[];
XYZ_feat_tables text[];
ZYX_feat_tables text[];
selected_feat_tables text[];
feature_list text[];
join_condition text;

BEGIN
current_date_str = to_char(current_date, 'DD_Mon_YYYY');
model_name = 'practice_mod';

select academic_period 
from oir.academic_calendar
where current_date >= start_date and current_date <= end_date
into current_term;

-- Group tables for refresh purpose
daily_refresh_tables = array[
    'features_dependent_variables.probation_dep_var',    --not a feature table!!
    'features_demographics.high_school',
    'features_demographics.high_school_inst_gpa',
    'features_demographics.high_school_core_gpa',
    'features_demographics.acs_hs_zip',
    'features_demographics.high_school_acceptance_rate'
    ];

census_refresh_tables = array[
    'features_demographics.feats_from_profile_table'
    ];

-- Group tables based on their sources
XYZ_feat_tables = array[
    'jkommera.dep_var',    --not a feature table!!
    'jkommera.personal_details',
    'jkommera.hs_inst_gpa',
    'jkommera.hs_core_gpa'
    ];

ZYX_feat_tables = array[
    'features_demographics.acs_hs_zip',
    'features_demographics.high_school_acceptance_rate'
    ];


--Combine selected feature tables
selected_feat_tables = null::text[];
FOR i in 1..array_upper(feat_sources, 1) LOOP
    if feat_sources[i] = 'XYZ' then selected_feat_tables = selected_feat_tables || XYZ_feat_tables;
    elseif feat_sources[i] = 'ZYX' then selected_feat_tables = selected_feat_tables || ZYX_feat_tables;
    end if;
end loop;

--Loop through the daily refresh tables, pull the last time they were updated, and update if needed
FOR i IN 1..array_upper(daily_refresh_tables, 1) LOOP
    select statime::date 
    from 
    (
        select t1.schemaname, t1.relname, t2.statime, row_number() over (partition by relname order by statime desc) 
        from  pg_stat_all_tables t1
        join pg_stat_last_operation t2 
        on t1.relid = t2.objid and
        t1.schemaname = substring(daily_refresh_tables[i] from '#"%#".%' for '#') and
        t1.relname = substring(daily_refresh_tables[i] from '%.#"%#"' for '#') and
        t2.staactionname = 'CREATE' and 
        t2.stasubtype = 'TABLE'
    )t
    where row_number = 1 
    into last_update_date;

    --can be changed to higher refresh frequency if needed
    --make sure update functions share the name of the table exactly!
    IF current_date - last_update_date > daily_table_waited_days THEN 
        execute $$select $$ || daily_refresh_tables[i] || $$()$$;
    ELSE CONTINUE;
    END IF;
END LOOP;

FOR i IN 1..array_upper(census_refresh_tables, 1) LOOP
    select statime::date 
    from 
    (
        select t1.schemaname, t1.relname, t2.statime, row_number() over (partition by relname order by statime desc) 
        from  pg_stat_all_tables t1
        join pg_stat_last_operation t2 
        on t1.relid = t2.objid and
        t1.schemaname = substring(census_refresh_tables[i] from '#"%#".%' for '#') and
        t1.relname = substring(census_refresh_tables[i] from '%.#"%#"' for '#') and
        t2.staactionname = 'CREATE' and 
        t2.stasubtype = 'TABLE'
    )t
    where row_number = 1 
    into last_update_date;

    select academic_period 
    from oir.academic_calendar
    where last_update_date >= start_date and last_update_date <= end_date
    into last_update_term;

    --if not the same term, refresh
    IF current_term != last_update_term THEN 
        execute $$select $$ || census_refresh_tables[i] || $$()$$;
    ELSE CONTINUE;
    END IF;
END LOOP;

--create a 'base' table
--REPLACE THIS WITH YOUR DEPENDENT VARIABLE
drop table if exists mod_temp_table cascade;
create temp table mod_temp_table as
(
    select * from jkommera.dep_var
) distributed by (person_uid);

FOR i in 1..array_upper(selected_feat_tables, 1) LOOP    
    -- Exclude non-feature tables    
    IF selected_feat_tables[i] not in (
        'jkommera.dep_var'
        ) THEN

        feature_list = null::text[];
        
        --select the names of all feature names into a list, excluding non-feature variables
        select array_agg(column_name::text) as column_name_arr 
        from information_schema.columns
        where 
            table_schema || '.' || table_name = selected_feat_tables[i] and
            column_name not in (
                'person_uid', 
                'id',
                'puid',
                'career_account',
                'academic_period',
                'profile_academic_period'
                ) and    
            --example of how to exclude all features starting with a string
            column_name !~ E'person\\_detail\\_.+'   --exclude person_detail_male and person_detail_urm columns in feats_from_person_detail_table
        into feature_list;

        --detect which type of join condition is required for this table based on the available data, and execute the join
        --make sure to add 'custom' ifs to the top, and edit this to be specific to your model.
        --the following sample set of conditions should give a good idea of how this works
        join_condition = null::text;

        IF selected_feat_tables[i] = 'features_financial_aid.feats_from_fin_aid_table_year_level' THEN
            join_condition = 't1.person_uid = t2.person_uid and substring(t1.academic_period::text from 3 for 2) = substring(t2.aid_year::text from 3 for 2)';
        ELSIF exists (select 1 from information_schema.columns where table_schema || '.' || table_name = selected_feat_tables[i] AND column_name='person_uid') = TRUE THEN
            IF exists (select 1 from information_schema.columns where table_schema || '.' || table_name = selected_feat_tables[i] AND column_name='academic_period') = TRUE THEN
                IF exists (select 1 from information_schema.columns where table_schema || '.' || table_name = selected_feat_tables[i] AND column_name='course_identification') = TRUE THEN
                    join_condition = 't1.person_uid = t2.person_uid and t1.academic_period = t2.academic_period and t1.course_identification = t2.course_identification';
                ELSE 
                    join_condition = 't1.person_uid = t2.person_uid and t1.academic_period = t2.academic_period';
                END IF;
            ELSIF exists (select 1 from information_schema.columns where table_schema || '.' || table_name = selected_feat_tables[i] AND column_name='course_identification') = TRUE THEN
                join_condition = 't1.person_uid = t2.person_uid and t1.course_identification = t2.course_identification';
            ELSE
                join_condition = 't1.person_uid = t2.person_uid';
            END IF;
        ELSIF exists (select 1 from information_schema.columns where table_schema || '.' || table_name = selected_feat_tables[i] AND column_name='academic_period') = TRUE THEN
            IF exists (select 1 from information_schema.columns where table_schema || '.' || table_name = selected_feat_tables[i] AND column_name='course_identification') = TRUE THEN
                join_condition = 't1.academic_period = t2.academic_period and t1.course_identification = t2.course_identification';
            ELSE 
                join_condition = 't1.academic_period = t2.academic_period';
            END IF;
        ELSIF exists (select 1 from information_schema.columns where table_schema || '.' || table_name = selected_feat_tables[i] AND column_name='course_identification') = TRUE THEN
            join_condition = 't1.course_identification = t2.course_identification';
        END IF;

        drop table if exists new_mod_temp_table cascade;
        EXECUTE $$create temp table new_mod_temp_table as
        (
            select 
                t1.*,
                $$ || array_to_string(feature_list, $a$, $a$) || $$
            from
                mod_temp_table t1
            left join
                $$ || selected_feat_tables[i] || $$ t2
            on $$ || join_condition || $$
        ) distributed by (person_uid)$$;

        drop table if exists mod_temp_table cascade;
        alter table new_mod_temp_table rename to mod_temp_table;

    END IF;
END LOOP;

EXECUTE $$drop table if exists $$ || output_schema || $$.$$ || model_name || $$_input_table_$$ || array_to_string(feat_sources, $$_$$) || $$_$$ || current_date_str || $$ cascade$$;
EXECUTE $$create table $$ || output_schema || $$.$$ || model_name || $$_input_table_$$ || array_to_string(feat_sources, $$_$$) || $$_$$ || current_date_str || $$ as
(
    select * from mod_temp_table
) distributed by (person_uid)$$;

END;
$BODY$
LANGUAGE PLPGSQL;

--select oir.tutorial_mod_input_table_build(array['XYZ','ZYX'], 'oir', 0);
