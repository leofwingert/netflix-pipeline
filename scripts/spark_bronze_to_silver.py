import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_extract, when, to_timestamp, coalesce

def main():
    # 1. Iniciar SparkSession
    spark = SparkSession.builder \
        .appName("NetflixDataLakeBronzeToSilver") \
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
    df_ratings_2 = spark.read.option("header", "true").csv(f"{BUCKET}/bronze/user_additional_rating.csv")
    
    # Une os dois datasets
    df_ratings_raw = df_ratings_1.unionByName(df_ratings_2)

    # Tratamento de NAs, conversão numérica e data
    df_ratings_clean = df_ratings_raw \
        .withColumn("user_id", col("userId").cast("long")) \
        .withColumn("movie_id", col("movieId").cast("long")) \
        .withColumn("rating", when(col("rating") == "NA", None).otherwise(col("rating")).cast("double")) \
        .withColumn(
            "rating_ts",
            coalesce(
                to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss"),
                to_timestamp(col("timestamp").cast("long"))
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