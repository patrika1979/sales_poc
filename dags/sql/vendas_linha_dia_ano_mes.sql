CREATE OR REPLACE TABLE `{{ params.dwh_dataset }}.vendas_linha_dia_nao_mes` AS
select sum(qtd_venda) as valor,  
       linha,
       EXTRACT(DAY FROM DATE (data_venda)) as day,
       EXTRACT(YEAR FROM DATE (data_venda)) as ano,
       EXTRACT(MONTH FROM DATE (data_venda)) as mes
from  `{{ params.project_id }}.{{ params.staging_dataset }}.vendas_staging` 
group by 2, 3, 4, 5