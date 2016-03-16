from kafka import SimpleProducer, KafkaClient
import time

kafka = KafkaClient('localhost:9092')
producer = SimpleProducer(kafka)
# qtdMensagens = 5820147
qtdMensagens = 4898431
qtdTopicos = 6
contadorTopicos = qtdTopicos - 1
while contadorTopicos < qtdTopicos:
    contadorTopicos += 1
    arquivo = open("amostras.txt", "r")
    contador = 0;
    while contador < qtdMensagens:
        topico = 'test' + str(contadorTopicos)
        topico = topico.encode('utf-8')
        mensagem = arquivo.readline()
        mensagem = mensagem.rstrip('\n').encode('utf-8')
        producer.send_messages(topico, mensagem)
        contador += 1;
        if contador % 1000 == 0:
            print contador
    arquivo.close()
# arquivo = open("amostras.txt", "r")
# contador = 0;
# while contador < qtdMensagens:
#     mensagem = arquivo.readline()
#     mensagem = mensagem.rstrip('\n').encode('utf-8')
#     producer.send_messages(b'test2', mensagem)
#     contador += 1;
#     if contador % 1000 == 0:
#         print contador
# arquivo.close()
# arquivo = open("amostras.txt", "r")
# contador = 0;
# while contador < qtdMensagens:
#     mensagem = arquivo.readline()
#     mensagem = mensagem.rstrip('\n').encode('utf-8')
#     producer.send_messages(b'test3', mensagem)
#     contador += 1;
#     if contador % 1000 == 0:
#         print contador
# arquivo.close()
