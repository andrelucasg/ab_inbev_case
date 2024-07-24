# André Lucas - Case AB Inbev

Olá pessoal,

- Considerações iniciais:
    - Apesar de já ter ouvido falar, eu nunca havia utilizado Docker e Airflow. Desde que trabalho com tecnologia, sempre utilizei Databricks para desenvolvimento e orquestração (em alguns casos usei Data Factory).
    - Foi um belo desafio, nesses dias aprendi bastante. O mais difícil é a questão das configurações... O Databricks já vem com o Spark rodando perfeitamente. No Docker precisa configurar Java, bibliotecas, garantir compatibilidade de bibliotecas...
    - Mesmo não sendo obrigatório, resolvi seguir com Docker pelo desafio e pelo aviso no documento de que ganharia "pontos extras".
    - Portanto, peço perdão se mencionar alguma nomenclatura incorreta ou padrão do Docker/Airflow que eu tenha feito incorretamente.

- Ao Projeto:
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
 
- Fluxograma:
![Output - Pipeline (1)](https://github.com/user-attachments/assets/8653a2f9-8b09-4525-bffa-f109c10406db)
