# kafka-streams-cloud-stream

- Existem três tipos principais no Kafka Streams - KStream, KTable e GlobalKTable. Spring Cloud Stream oferece suporte a todos eles. Podemos facilmente converter o stream em tabela e vice-versa. Para esclarecer, todos os tópicos do Kafka são armazenados como um fluxo. A diferença é: quando queremos consumir esse tópico, podemos consumi-lo como uma tabela ou como um stream. O KTable pega um fluxo de registros de um tópico e o reduz a entradas únicas usando uma chave de cada mensagem.

## Operações do kafka streams
