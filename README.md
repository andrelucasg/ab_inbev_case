# André Lucas - Case AB Inbev

Olá pessoal,

- Considerações iniciais:
   - Apesar de já ter ouvido falar, eu nunca havia utilizado Docker e Airflow. Desde que trabalho com tecnologia, sempre utilizei Databricks para desenvolvimento e orquestração (em alguns casos usei Data Factory).
   - Foi um belo desafio, nesses dias aprendi bastante. O mais difícil é a questão das configurações... O Databricks já vem com o Spark rodando perfeitamente. No Docker precisa configurar Java, bibliotecas, garantir compatibilidade de bibliotecas...
   - Mesmo não sendo obrigatório, resolvi seguir com Docker pelo desafio e pelo aviso no documento de que ganharia "pontos extras".
   - Portanto, peço perdão se mencionar alguma nomenclatura incorreta ou padrão do Docker/Airflow que eu tenha feito incorretamente.

- Projeto:
   - Como mencionado nas considerações iniciais, resolvi seguir com Docker e Airflow. Além deles, decidi por seguir o código com Pyspark e utilizar Azure como plataforma em nuvem.
   - De maneira bem generalizada (vou detalhar logo abaixo), são 4 tasks:
       - Raw: Faz a consulta na api e armazena o JSON retornado;
       - Bronze: Recebe o retorno do JSON armazenado pela task Raw, cria o Dataframe e armazena em Delta na Azure;
       - Silver: Faz a leitura da camada Bronze, enriquece com uma nova coluna, padroniza os valores e salva de forma particionada na Azure em formato Delta;
       - Gold: Faz a leitura da camada Silver, agrupa fazendo a contagem de cervejarias e salva na Azure em formato Delta.

- Monitoramento/Falhas/Padrões:
   - No código eu praticamente trabalho com Pyspark e uso o formato Delta para manipular na Azure. Costumo usar Delta pela facilidade em versionamento. Caso seja necessário restaurar alguma versão anterior por algum motivo específico, basta selecionar a versão e restaurar o arquivo normalmente;
   - Pesquisando sobre o Airflow, gostaria de usar a funcionalidade de disparar e-mail em caso de falha. Não acho necessário disparar e-mail para tasks que rodaram com sucesso, afinal esse é o resultado esperado. Mas quando há falha, acredito que seria uma boa prática manter essa funcionalidade ativa.
   - Acredito que construiría algo parecido com o que montei onde trabalho. Uso o Power Automate para atualizar as execuções diárias diretamente no Teams. Por ser mais rápido e direto que e-mail, o retorno automático via Teams acaba sendo bem efetivo.
 
1 - Dockerfile
   - Instala JDK 17;
   - Instala bibliotecas necessárias para o código;
   - Baixa os JARS necessários para o ambiente Spark;
   - Define o caminho correto para cada JAR;
   - Comentário: Provavelmente existem mais JARS que o necessário aqui. Isso é devido a algumas tentativas de preparar o ambiente Spark da forma que eu queria.

2 - Azure
   - Criei uma conta na Azure com um e-mail pessoal para esse case;
   - Após criado o Storage, criei um Container e em sequência a SAS Key para me conectar via código;
   - Na sessão Spark, preencho as informações necessárias para a conexão (account key e sas token).

3 - Código
   - 3.1 Fluxograma:
![Output - Pipeline (1)](https://github.com/user-attachments/assets/8653a2f9-8b09-4525-bffa-f109c10406db)

   - 3.2 global spark:
       - Variável global criada responsável por criar a sessão Spark que será utilizada no decorrer do código;
       - Define os JARS separadamente para serem configurados na sessão quando criada;
       - Na criação da sessão spark, as seguintes configurações são realizadas:
         - .config("spark.jars", ",".join(jars)): JARS que serão usados na sessão;
         - .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"): Configuração necessária para usar Delta;
         - .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"): Configuração necessária para usar Delta;
         - .config("fs.azure.account.key.storagecaseabinbev.dfs.core.windows.net", "key"): Chave da conta Azure;
         - .config("fs.azure.sas.fixed.token.storagecaseabinbev.dfs.core.windows.net", "sas"): Chave SAS;

   - 3.3 global container_client:
       - Variável global criada responsável por configurar o acesso ao container da azure;
       - Cria a variável "azure_path_with_sas" que é usada no restante do código para ler e gravar na Azure;

   - 3.4 def azure_storage:
      - Função criada responsável por toda interação de leitura e escrita na Azure;
      - Recebe os seguintes parâmetros:
        - action: "upload" para enviar dado para Azure; "read" para fazer leitura na Azure;
        - data: base/dataframe de dados;
        - df_schema: schema se houver;
        - blob_prefix: qual o blob/diretório que a interação irá acontecer seja para leitura ou gravação (podendo ser bronze, silver e gold).

   - 3.4.1 def send_storage:
      - Função internalizada na função azure_storage;
      - Caso a action chamada tenha sido "upload", a função é acionada;
      - Verifica o "blob_prefix". Se for "bronze", cria-se o dataframe spark e salva em formato delta diretamente no blob "bronze"; Se for "silver", salva em formato delta no blob "silver". Se for "gold", salva em formato delta no blob "gold".
    
   - 3.4.2 def read_from_storage:
      - Função internalizada na função azure_storage;
      - Caso a action chamada tenha sido "read", a função é acionada;
      - Dependendo do "blob_prefix" acionado, faz a leitura e retorna o dataframe lido. Se for "bronze", faz a leitura do blob que estão os arquivos delta da camada "bronze" e retorna o dataframe. Para "silver" e "gold", segue o mesmo padrão.

   - 3.5 def raw_def:
      - Função responsável por consultar e extrair os dados da API;
      - Retorna os dados coletados;
    
   - 3.6 def bronze_def:
      - Função responsável pela camada bronze;
      - Faz xpull do resultado da função raw_def;
      - Define o schema manualmente;
      - Chama a função azure_storage com o action definido para "upload" e blob_prefix "bronze". Ou seja, faz o upload/escrita do dataframe delta na camada bronze;

   - 3.7 def silver_def:
      - 
