from config.spark_config import SparkConnect

def main():

    spark_connect = SparkConnect(
        app_name="bangg",
        master_url="local[*]",
        executor_memory="2g",
        executor_cores=1,
        driver_memory="2g",
        num_executors=1,
        log_level="DEBUG"
    )

    data = [["bang",18],
            ["virus", 30],
            ["jack", 28]]

    df = spark_connect.spark.createDataFrame(data, ["name", "age"])
    df.show()

if __name__ == "__main__":
    main()