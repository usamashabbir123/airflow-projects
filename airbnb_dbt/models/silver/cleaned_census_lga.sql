{{
    config(
        materialized='table'
    )
}}
SELECT SUBSTRING(lga_code_2016 FROM 4) as lga_code,*
from 
{{source('bronze','g02')}}
inner join 
{{source('bronze','g01')}} 
 using ( lga_code_2016)