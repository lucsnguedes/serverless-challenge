# coding=utf-8
import json #Formato leve de troca de informações/dados entre sistemas
import boto3 #Facilita a integração de sua aplicação, biblioteca ou script Python aos serviços da AWS
from PIL import Image #Biblioteca para trabalhar com imagens

dynamodb = boto3.client("dynamodb") #Conexão com o banco de dados e definição do client "dynamodb"
s3 = boto3.client('s3') #Conexão e definição do s3

def getImage(lg-bucket, s3objectkey): #Recebendo imagem armazenada no S3

    obj = s3.Object(bucket_name=lg-bucket, key=s3objectkey) #Recebendo o objeto com a chave(id) inserido no bucket
    objBody = obj.get()['Body'].read() #Extraindo informal
    img = Image.open(objBody)
    width = img.size[0] #Definindo dimensões da imagem
    height = img.size[1]
    
def extractMetadata(event, context): #'def' define a função e o parâmetro 'event' dispara a função a cada novo evento(upload) feito no S3
    
    record = event['Records']
    lg-bucket = record ['s3']['bucket']['name'] #Construindo variável e obtendo os registros das informações pedidas entre colchetes de forma organizada  
    s3objectkey = record ['s3']['object']['key'] #Imagem
    size = Record ['s3']['object']['size'] #Extraindo tamanho da imagem
    width, height = getImage(lg-bucket, s3objectkey) #Extraindo as dimensões da imagem
    
    table = dynamodb.Table('images') #Tabela do banco de dados   
    table.put_item( #Inserindo o item no banco de dados
        Item={  #Definindo as informações do item que vão ser inseridas no banco de dados
            's3objectkey': s3objectkey,
            'body': width, height, size
             }
          )

 
def getMetadata(event, context): #'def' define a função e o parâmetro 'event' dispara a função a cada novo evento(upload) feito no S3
    
    s3objectkey = event['pathParameters']['s3objectkey'] #Construindo variável e obtendo os registros das informações pedidas entre colchetes de forma organizada
    dynamodb = boto3.client('dynamodb') #banco de dados
    item = dynamodb.get_item( #Puxando item do banco de dados
        TableName='images', #Tabela do banco de dados
        Key={
            's3objectkey': s3objectkey
            } 
    )

    width = item['Item']['body']['width']['S'] # 'S' Define o valor recebido como string 
    height =  item['Item']['body']['height']['S']
    size = item['Item']['body']['size']['S']
    format_img = item['Item']['Body']['format']['S']
    

    return {  #Retorna as respostas requeridas
        'statusCode': '200',
        'body': {'width': width, 'height': height, 'size': size, 'format_img': format_img}
    }
