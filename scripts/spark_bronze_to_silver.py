import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_extract, when, to_timestamp, coalesce

def main():
    # 1. Iniciar SparkSession
    # Conector GCS (instalado no Dockerfile) + autenticação via service account
    gcs_jar = os.getenv("GCS_CONNECTOR_JAR", "/opt/spark-jars/gcs-connector.jar")
    keyfile = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

    spark = SparkSession.builder \
        .appName("NetflixDataLakeBronzeToSilver") \
        .master("local[1]") \
        .config("spark.driver.memory", "512m") \
        .config("spark.driver.memoryOverhead", "128m") \
        .config("spark.driver.extraJavaOptions", "-XX:MaxMetaspaceSize=128m -XX:+UseSerialGC -Xss512k") \
        .config("spark.hadoop.fs.gs.outputstream.upload.chunk.size", "8388608") \
        .config("spark.hadoop.fs.gs.inputstream.min.range.request.size", "1048576") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .config("spark.hadoop.parquet.block.size", "33554432") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.sql.files.maxPartitionBytes", "64m") \
        .config("spark.ui.enabled", "false") \
        .config("spark.jars", gcs_jar) \
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", keyfile) \
        .getOrCreate()

    # Caminhos no bucket (ajuste o nome do bucket se necessário)
    BUCKET = "gs://leofwingert-netflix-bucket"
    
    print("Processando movies.csv...")
    # 2. Leitura e Limpeza de Filmes
    df_movies = spark.read.option("header", "true").csv(f"{BUCKET}/bronze/movies.csv")
    
    df_movies_clean = df_movies \
        .withColumn("movie_id", col("movieId").cast("long")) \
        .withColumn("title", col("title")) \
        .withColumn("genres", col("genres")) \
        .withColumn("release_year", regexp_extract(col("title"), r"\((\d{4})\)\s*$", 1).cast("int")) \
        .select("movie_id", "title", "genres", "release_year")
    
    # Salvar em Silver no formato colunar Parquet
    df_movies_clean.write.mode("overwrite").parquet(f"{BUCKET}/silver/movies/")

    print("Processando avaliações (ratings)...")
    # 3. Leitura e Limpeza de Ratings (Histórico + Adicional)
    df_ratings_1 = spark.read.option("header", "true").csv(f"{BUCKET}/bronze/user_rating_history.csv")
    df_ratings_2 = spark.read.option("header", "true").csv(f"{BUCKET}/bronze/ratings_for_additional_users.csv")
    
    # Une os dois datasets
    df_ratings_raw = df_ratings_1.unionByName(df_ratings_2)

    # Tratamento de NAs, conversão numérica e data (coluna original: tstamp)
    df_ratings_clean = df_ratings_raw \
        .withColumn("user_id", col("userId").cast("long")) \
        .withColumn("movie_id", col("movieId").cast("long")) \
        .withColumn("rating", when(col("rating") == "NA", None).otherwise(col("rating")).cast("double")) \
        .withColumn(
            "rating_ts",
            coalesce(
                to_timestamp(col("tstamp"), "yyyy-MM-dd HH:mm:ss"),
                to_timestamp(col("tstamp").cast("long"))
            )
        ) \
        .filter(
            col("user_id").isNotNull() & 
            col("movie_id").isNotNull() & 
            col("rating").isNotNull() & 
            col("rating_ts").isNotNull()
        ) \
        .select("user_id", "movie_id", "rating", "rating_ts")

    # Salva ratings em Silver como Parquet particionado (otimização de Big Data)
    df_ratings_clean.write.mode("overwrite").parquet(f"{BUCKET}/silver/ratings/")

    print("Processamento concluído com sucesso!")
    spark.stop()

if __name__ == "__main__":
    main()