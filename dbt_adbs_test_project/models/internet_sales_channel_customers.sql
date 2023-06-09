{#
 Copyright (c) 2022, Oracle and/or its affiliates.

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

     https://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
#}
{{config(materialized='table', parallel=4, table_compression_clause='COLUMN STORE COMPRESS FOR QUERY')}}
select c.cust_id, c.cust_first_name, c.cust_last_name, t.country_iso_code, t.country_name, t.country_region
from {{ ref('sales_internet_channel') }} s, {{ source('sh_database', 'countries') }} t, {{ source('sh_database', 'customers') }} c
WHERE s.cust_id = c.cust_id
AND c.country_id = t.country_id
