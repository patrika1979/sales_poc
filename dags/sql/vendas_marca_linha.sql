CREATE OR REPLACE TABLE `{{ params.dwh_dataset }}.vendas_marca_linha` AS
select sum(qtd_venda) as valor,  
       marca,
       linha
from  `{{ params.project_id }}.{{ params.staging_dataset }}.vendas_staging` 
group by 2, 3