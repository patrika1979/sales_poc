CREATE TABLE IF NOT EXISTS `{{ params.dwh_dataset }}.vendas_marca_linha` AS
select sum(qtd_venda) as valor,  
       marca,
       linha
from  `{{ params.project_id }}.{{ params.staging_dataset }}.vendas_staging` 
WHERE 1=2
group by 2, 3;

INSERT INTO `{{ params.dwh_dataset }}.vendas_marca_linha`
select sum(qtd_venda) as valor,  
       marca,
       linha
from  `{{ params.project_id }}.{{ params.staging_dataset }}.vendas_staging` 
group by 2, 3;