CREATE TABLE IF NOT EXISTS `{{ params.dwh_dataset }}.vendas_marca_ano_mes` AS
select sum(qtd_venda) as valor,  
       marca,
       EXTRACT(YEAR FROM DATE (data_venda)) as ano,
       EXTRACT(MONTH FROM DATE (data_venda)) as mes
from  `{{ params.project_id }}.{{ params.staging_dataset }}.vendas_staging` 
WHERE 1=2
group by 2, 3, 4;

INSERT INTO `{{ params.dwh_dataset }}.vendas_marca_ano_mes`
select sum(qtd_venda) as valor,  
       marca,
       EXTRACT(YEAR FROM DATE (data_venda)) as ano,
       EXTRACT(MONTH FROM DATE (data_venda)) as mes
from  `{{ params.project_id }}.{{ params.staging_dataset }}.vendas_staging` 
group by 2, 3, 4;