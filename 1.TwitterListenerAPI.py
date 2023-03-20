from tweepy.streaming import StreamListener
from tweepy import Stream
from tweepy import OAuthHandler

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql import functions as F

import json
import time

class Tweet():
    """
    Objeto com os atributos refrentes aos dados a serem extraidos do JSON da API do Twitter
    """
    tweet_id: str = None
    created_at: str = None
    text: str = None
    hashtags: str = None
    retweet_count: int = None
    possibly_sensitive: bool = None
    lang: str = None
    user_id: str = None
    user_name: str = None
    user_description: str = None
    user_verification: bool = None
    user_followers_count: int = None
    user_friends_count: int = None
    user_created_at: str = None
    user_location: str = None
    
    def get_list(self):
        """
        Retorna uma lista com os valores atribuidos ao objeto instanciado
        """
        list_data = [
            self.tweet_id,
            self.created_at,
            self.text,
            self.hashtags,
            self.retweet_count,
            self.possibly_sensitive,
            self.lang,
            self.user_id,
            self.user_name,
            self.user_description,
            self.user_verification,
            self.user_followers_count,
            self.user_friends_count,
            self.user_created_at,
            self.user_location,
        ]
        return list_data
    
    def insert(self, data):
        """
        Recebe o retorno da API e insere os dados em seus respectivos atributos
        """
        hashtags = ""
        if "extended_tweet" in data:
            for h in data['extended_tweet']["entities"]["hashtags"]:
                hashtags = h["text"] + ", " + hashtags
        else:
            for h in data["entities"]["hashtags"]:
                hashtags = h["text"] + ", " + hashtags
        self.tweet_id = data["id"]
        self.created_at = data["created_at"]
        self.text = data['extended_tweet']['full_text'] if "extended_tweet" in data else data["text"]
        self.user_id = data["user"]["id"]
        self.hashtags = hashtags
        self.retweet_count = data["retweet_count"]
        self.possibly_sensitive = data['possibly_sensitive'] if "possibly_sensitive" in data else None
        self.lang = data["lang"]
        self.user_id = data["user"]["id"]
        self.user_name = data["user"]["name"]
        self.user_description = data["user"]["description"]
        self.user_verification = data["user"]["verified"]
        self.user_followers_count = data["user"]["followers_count"]
        self.user_friends_count = data["user"]["friends_count"]
        self.user_created_at = data["user"]["created_at"]
        self.user_location = data["user"]["location"]

class TwitterListener(StreamListener):
    """
    Listener da API do Twitter, esta classe realiza o streaming de tweets,
    recebendo os tweets, processando e persistindo em uma tabela temporaria.
    """
    def __init__(self, persist_time):
        self.persist_time = persist_time
        self.start = time.time()
        self.listTweets = []
        self.schema = StructType([StructField("tweet_id", StringType(), True),
                                  StructField("created_at", StringType(), True),
                                  StructField("text", StringType(), True),
                                  StructField("hashtags", StringType(), True),
                                  StructField("retweet_count", IntegerType(), True),
                                  StructField("possibly_sensitive", BooleanType(), True),
                                  StructField("lang", StringType(), True),
                                  StructField("user_id", StringType(), True),
                                  StructField("user_name", StringType(), True),
                                  StructField("user_description", StringType(), True),
                                  StructField("user_verification", BooleanType(), True),
                                  StructField("user_followers_count", IntegerType(), True),
                                  StructField("user_friends_count", IntegerType(), True),
                                  StructField("user_created_at", StringType(), True),
                                  StructField("user_location", StringType(), True),])    
    
    def on_data(self, data):
        """
        Metodo que irá receber o dados da API e apos decorrido o tempo,
        passado no parametro 'persist_time', ira salva-los em uma tabela temporaria.
        """
        try:
            tweet = Tweet()
            json_data = json.loads(data)
            if "limit" not in json_data:
                tweet.insert(json_data)
                self.listTweets.append(tweet.get_list())
                
                print(f"Tweets:{len(self.listTweets)}, Time:{(time.time() - self.start)}")
                if (time.time() - self.start) > self.persist_time:
                    try:
                        df = sqlContext.createDataFrame(data=self.listTweets, schema=self.schema)
                        df = df.withColumn("etl_load", F.current_timestamp())
                        df = df.withColumn("etl_load_partition_year", F.date_format("etl_load", "yyyy"))
                        df = df.withColumn("etl_load_partition_month", F.date_format("etl_load", "MM"))
                        df = df.withColumn("etl_load_partition_day", F.date_format("etl_load", "dd"))
                        df = df.withColumn("etl_load_partition_hour", F.date_format("etl_load", "HH"))
                        df.createOrReplaceTempView("tweets")
                    except BaseException as e:
                        print("Erro ao contruir o dataframe 'df': " + str(e))
                    return False
        except BaseException as e:
            print("Error: " + str(e), "JSON com layout fora do esperado:", json_data)
        return True
    
    def on_error(self, status_code):
        print("Error: " + status_code)
        return True
    
    def on_timeout(self):
        print("Timeout!")
        return True

def authentication():
    """
    Busca as credenciais em uma pasta e retorna o objeto auth:OAuthHandler
    """
    PATH = "C:/Users/Adelso Silva/Documents/GitHub/TwitterAPI/{0}"
    
    API_KEY = open(file=PATH.format("API_key.txt"), mode="r", encoding="UTF-8").read()
    API_SECRET_KEY = open(file=PATH.format("API_secret_key.txt"), mode="r", encoding="UTF-8").read()
    ACCESS_TOKEN = open(file=PATH.format("access_token.txt"), mode="r", encoding="UTF-8").read()
    ACCESS_TOKEN_SECRET = open(file=PATH.format("access_token_secret.txt"), mode="r", encoding="UTF-8").read()
    
    auth = OAuthHandler(API_KEY, API_SECRET_KEY)
    auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
    return auth

def getData(keywords, languages=None, timeout=None, persist_time=60):
    """
    Metodo que ira realizar os passo-a-passo de autenticacao, conifguracao e start do streamig
    e retornar os dados salvos na tabela temporaria assim que decorrido o tempo do parametro 'persist_time'.
    """
    print('authenticating...')
    auth = authentication()
    print('start twitter listener...')
    twitter_listener = TwitterListener(persist_time=persist_time)
    print('start twitter stream...')
    twitter_stream = Stream(auth=auth, listener=twitter_listener, timeout=timeout)
    try:
        print("getting data...")
        twitter_stream.filter(track=keywords, is_async=False, languages=languages)
        twitter_stream.disconnect()
        return sqlContext.sql("select * from tweets")
    except BaseException as e:
        print("Error: " + str(e))

if __name__ == "__main__":
    sc = SparkContext.getOrCreate()
    spark = SparkSession.builder.getOrCreate()
    sqlContext = SQLContext(sc)
    
    keywords = ["COVID", "CORONAVIRUS", "SARS-COV-2"]
    languages=["pt", "en"]
    timeout = 60 ### Verificar pois apresenta inconsistencias
    persist_time = 10
    path = "./layer_bronze/tweets"

    while True:
        df = getData(keywords=keywords,
                     languages=languages,
                     timeout=timeout, ### Verificar pois apresenta inconsistencias
                     persist_time=persist_time,
                    )
        
        print("saving parquet...")
        df.write\
          .partitionBy("etl_load_partition_year",
                       "etl_load_partition_month",
                       "etl_load_partition_day",
                       "etl_load_partition_hour")\
          .format("parquet")\
          .mode("append")\
          .save(path)