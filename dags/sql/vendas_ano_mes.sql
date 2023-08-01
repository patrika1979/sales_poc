CREATE OR REPLACE TABLE `{{ params.dwh_dataset }}.F_IMMIGRATION_DATA` AS
select sum(qtd_venda) as valor,  
       EXTRACT(YEAR FROM DATE (data_venda)) as ano,
       EXTRACT(MONTH FROM DATE (data_venda)) as mes
from  `{{ params.project_id }}.{{ params.staging_dataset }}.vendas_staging` 
group by 2, 3