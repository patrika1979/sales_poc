from google.cloud.bigquery import SchemaField 

tables = ['vendas_staging']

vendas_staging= [SchemaField('ID_MARCA', 'STRING', 'NULLABLE', None, ())
,SchemaField('MARCA', 'STRING', 'NULLABLE', None, ())
,SchemaField('ID_LINHA', 'STRING', 'NULLABLE', None, ())
,SchemaField('LINHA', 'INTEGER', 'NULLABLE', None, ())
,SchemaField('DATA_VENDA', 'STRING', 'NULLABLE', None, ())
,SchemaField('QTD_VENDA', 'STRING', 'NULLABLE', None, ())
]
