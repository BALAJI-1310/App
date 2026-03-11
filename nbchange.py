try:
    print("STEP 3: Writing Segment Table")

    abfs_path = f"abfss://{StorageContainerName}@{StorageAccountName}.dfs.core.windows.net/Table"
    print(SegmentQuery)

    df = spark.sql(SegmentQuery)

    spark.sql("CREATE SCHEMA IF NOT EXISTS Segmentation")

    segment_id_clean = segment_id.lower().replace("-", "_")

    result_table = f"Segmentation.segment_{segment_id_clean}"


    def write_operation():

        (
            df.write
            .format("delta")
            .mode("overwrite")
            .option("path", abfs_path)
            .option("overwriteSchema","true")
            .saveAsTable(result_table)
        )


    # retry logic
    retry_count = 0
    max_retries = 5

    while retry_count < max_retries:
        try:
            write_operation()
            print("Table created:", result_table)
            break

        except Exception as write_error:

            if "concurrent" in str(write_error).lower() or "files were added to the root" in str(write_error).lower():
                retry_count += 1
                print(f"Concurrent write detected. Retrying {retry_count}/{max_retries}...")
                import time
                time.sleep(10)

            else:
                raise write_error

    if retry_count == max_retries:
        raise Exception("Max retries reached while writing segment table")

except Exception as e:
    exit_failure("SEGMENT TABLE WRITE FAILED", e)
