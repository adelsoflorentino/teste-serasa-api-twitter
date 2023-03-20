###### Case Serasa Experian ######


###### 1 - O Case:

## Neste case, você precisa implementar uma solução que: 
- Consiga buscar tweets com uma determinada “HashTag”, por exemplo, #covid19;
- Armazenar os resultados em formato Parquet;
- Estruture um datalake para que seja possível consolidar dados analíticos por hashtag e posteriormente consultar informações coletadas de forma batch;

## Como sugestão você poderia usar:
- Spark/Scala – Para extração, escrita e consolidação;
- Airflow – Orquestração do processamento;
- Docker/Compose – Para deploy da aplicação;

## Tópicos que consideramos relevantes:
- Testes unitários;
- Estrutura de código;
- Boas práticas de Software Engineering;
- Boas práticas de Data Engineering;


###### 2 - A Solução:

	Esta aplicação foi desenvolvida em Python utilizando [Pyspark](https://spark.apache.org/docs/latest/api/python/index.html) 
e [Tweepy](https://www.tweepy.org/) para conexão com a API do Twitter.

	A aplicação foi dividida em duas etapas:

--- 1º Etapa
	
	[1.TwitterListenerAPI.py](https://github.com/adelsoflorentino/teste-serasa-api-twitter/blob/main/1.TwitterListenerAPI.py)
	
	Realiza o streaming de tweets em tempo real no qual dada uma lista de *keywords* a serem buscados em tweets, a aplicação irá:
	
- Realizar a conexão com a API de streaming do Twitter;
- Baixar os tweets em tempo real;
- Salvá-los em uma *layer_bronze* de dados de forma particionada e em arquivos no formato parquet.

--- 2º Etapa

	[2.DataLake.py](https://github.com/adelsoflorentino/teste-serasa-api-twitter/blob/main/2.DataLake.py)
	
	Criar um Datalake com os dados salvos na *layer_bronze* no qual sua função é a cada 60 minutos:

- Ler os dados salvos na *layer_bronze* na última hora;
- Realizar os devidos tratamentos dos dados como:
  - Cast das colunas para seus respectivos tipos de dados, `Integer`, `Boolean`, `Timestamp` e `String`;
  - Ajuste de fuso horário das colunas de timestamp para "America/Sao_Paulo";
- Criar as tabelas fato e dimensão;
- Salvá-las de forma particionada e em formato parquet numa *layer_silver* de dados.


--- Testes

	[3.DataLake_Viz.ipynb](https://github.com/adelsoflorentino/teste-serasa-api-twitter/blob/main/3.DataLake_Viz.ipynb)
	
	Testes unitarios realizados.


###### 3 - Pontos a serem implementados:

- A implementação de um orquestrador do fluxo de dados (Airflow), conforme sugerido no case;
- A criação de um ambiente Docker/Compose para deploy da aplicação, conforme sugerido no case;


###### 4 -  Melhorias:

- Alterar armazenamento do Datalake para o S3, com a premissas de camadas de dados.


###### 5 - Arquitetura inicial:

	[Arquitetura](https://github.com/adelsoflorentino/teste-serasa-api-twitter/blob/main/Arquitetura.png)

