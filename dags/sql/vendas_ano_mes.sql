CREATE TABLE IF NOT EXISTS `{{ params.dwh_dataset }}.vendas_ano_mes` AS
select sum(qtd_venda) as valor,  
       EXTRACT(YEAR FROM DATE (data_venda)) as ano,
       EXTRACT(MONTH FROM DATE (data_venda)) as mes
from  `{{ params.project_id }}.{{ params.staging_dataset }}.vendas_staging` 
WHERE 1=2
group by 2, 3;

INSERT INTO `{{ params.dwh_dataset }}.vendas_ano_mes`
select sum(qtd_venda) as valor,  
       EXTRACT(YEAR FROM DATE (data_venda)) as ano,
       EXTRACT(MONTH FROM DATE (data_venda)) as mes
from  `{{ params.project_id }}.{{ params.staging_dataset }}.vendas_staging`
group by 2, 3;