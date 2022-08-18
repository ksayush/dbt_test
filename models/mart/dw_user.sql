WITH cte_source AS (

  SELECT * FROM {{ ref('stg_user') }}

)

SELECT
         user_id
        ,load_date
        ,resource_type
        ,user_ext_uid
        ,user_email
        ,user_name
        ,user_role
        ,user_role_id
        ,user_archived_at
        ,user_archived_by_user_id
        ,user_job_title
        ,user_business_unit
        ,user_department
        ,user_location
        ,user_locale
        ,user_hire_date
        ,user_manager_name
        ,user_mobile_phone_number
FROM 
        cte_source