from pyspark.sql import SparkSession
from pyspark.sql import functions as f
import argparse

if __name__ == "__main__" :
    spark = SparkSession.builder.appName("Cleansing_Job").getOrCreate()

    parser = argparse.ArgumentParser()

    parser.add_argument("--input_path", required=True)
    parser.add_argument("--output_path", required=True)

    args = parser.parse_args()
    INPUT_PATH = args.input_path
    OUTPUT_PATH = args.output_path

    df = spark.read.csv(INPUT_PATH, header=True, inferSchema=True)
    
    #Specify the number of columns to move.
    df_move = df.withColumn("num_c", f.when(f.col("release_date").rlike("^[0-9]{4}[-][0-9]{2}[-][0-9]{2}$"), 0)
                                        .otherwise(f.when(f.col("required_age").rlike("^[0-9]{4}[-][0-9]{2}[-][0-9]{2}$"), 1)
                                        .otherwise(f.when(f.col("price").rlike("^[0-9]{4}[-][0-9]{2}[-][0-9]{2}$"),2))
                                                )
                                )
    #Loop to move data to match column
    col_names = df_move.columns
    for n in range(len(col_names)):
        #Will start editing at index column 2
        if n > 1:
            if n+2 < len(col_names):
                df_move = df_move.withColumn(col_names[n], f.when(f.col("num_c") == 0, f.col(col_names[n]))
                                                                .otherwise(f.when(f.col("num_c") == 1, f.col(col_names[n+1]))
                                                                .otherwise(f.when(f.col("num_c") == 2, f.col(col_names[n+2])))
                                                            )
                                            )
            elif n+1 < len(col_names):
                df_move = df_move.withColumn(col_names[n], f.when(f.col("num_c") == 0, f.col(col_names[n]))
                                                                .otherwise(f.when(f.col("num_c") == 1, f.col(col_names[n+1]))
                                                                .otherwise(1)
                                                            )
                                            )
            else:
                df_move = df_move.withColumn(col_names[n], f.when(f.col("num_c") == 0, f.col(col_names[n])).otherwise(1))
    
    #Select Column
    df_select = df_move.select("AppID","name","release_date","required_age","price","achievements","recommendations",
                               "user_score","positive","negative","average_playtime_forever","median_playtime_forever",
                               "peak_ccu","num_reviews_total")
    
    #Clean Missing Values
    col_names_ = df_select.columns
    for n in range(len(col_names_)):
        if n > 4:
            df_select = df_select.withColumn(col_names_[n], f.when(f.col(col_names_[n]).rlike("^[0-9.]+$"), 
                                                                   f.col(col_names_[n])).otherwise(0))
    df_clean = df_select.na.fill("Missing Values")
    
    #Change Type Data
    df_clean_date = df_clean.withColumn("new_release_date", f.to_timestamp(df_clean.release_date, "yyyy-MM-dd")).drop("release_date")
    
    new_column_type = {
        "required_age":"int",
        "price":"float",
        "achievements":"int",
        "recommendations":"int",
        "user_score":"int",
        "positive":"int",
        "negative":"int",
        "average_playtime_forever":"int",
        "median_playtime_forever":"int",
        "peak_ccu":"int",
        "num_reviews_total":"int"
    }
    df_clean_type = df_clean_date
    for col_name, col_type in new_column_type.items():
        df_clean_type = df_clean_type.withColumn(col_name, f.col(col_name).cast(col_type))
    
    #Rename Column
    df_final = df_clean_type.withColumnRenamed("AppID","id").withColumnRenamed("new_release_date","release_date")
    
    #Output
    df_final.coalesce(1).write.parquet(OUTPUT_PATH, mode="overwrite")
    print("Transform Complete")
    spark.stop()