# Computação Escalável - Avaliação 2

Trabalho de computação escalável A2: Projeto de ETL e Dashboard para um sistema de monitoramento de rodovias. Trata-se de um sistema focado em escalabilidade e eficiência, usando Python, Redis e Dash.

## Integrantes
 - Breno Marques Azevedo
 - Bruno Pereira Fornaro
 - Luis Fernando Laguardia
 - Vanessa Berwanger Wille
 - Vinicius Hedler

## Executando o código localmente

Antes de executar o código localmente, é necessário ter um servidor Redis em execução. Recomenda-se utilizar o Docker e rodar a imagem oficial do Redis na porta 6379. Após configurar o servidor, siga os passos abaixo:

1. Certifique-se de que o servidor Redis está em execução no seu ambiente local.
2. Abra cinco prompts de comando distintos na pasta do repositório.
3. Execute os seguintes arquivos em cada prompt:

- **Simulador/Publisher:**
    - `python Simulator\simulator.py`
    - Esse arquivo inicia o simulador e publica os dados dos carros.

- **Subscriber:**
    - `python etl\python_to_db.py`
    - Esse arquivo contém o subscriber que recebe os dados do simulador e os salva no banco de dados.

- **Transformer:**
    - `python etl\db_to_spark.py`
    - Esse arquivo contém o transformador que lê os dados do banco, realiza as análises e os envia para o banco de dados do dashboard.

- **Loader:**
    - `python Dashboard\dashboard_data_collector.py`
    - Esse arquivo coleta os dados do banco de dados e os envia para o dashboard.

- **Dashboard:**
    - `python Dashboard\dashboard.py`
    - Esse comando executa a interface visual do projeto.

Certifique-se de executar cada arquivo em um prompt de comando separado para garantir o funcionamento adequado do sistema.


## Link vídeo

Foi feito um vídeo mostrando a execução do código. O vídeo pode ser encontrado em: https://vimeo.com/840328415/0043f663a3