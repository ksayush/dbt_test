{{ config(
  materialized='incremental',
  unique_key='user_id',
  file_format='delta'
) }}



with cte as(
select explode(users) as users_exploded, load_date from ayush.raw_users
{% if is_incremental() %}

    WHERE load_date > (SELECT max(load_date) FROM {{ this }})

{% endif %}

),

cte1 as(
select users_exploded.*,load_date from cte)

select id as user_id
      , resource_type
      , ext_uid as user_ext_uid
      , email as user_email
      , name as user_name
      , role as user_role
      , role_id as user_role_id
      , archived_at as user_archived_at
      , archived_by_user_id as user_archived_by_user_id
      , job_title as user_job_title
      , business_unit as user_business_unit
      , department as user_department
      , location as user_location
      , locale as user_locale
      , hire_date as user_hire_date
      , manager_name as user_manager_name
      , mobile_phone_number as user_mobile_phone_number
      , load_date
from cte1;