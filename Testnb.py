from notebookutils import mssparkutils
import json

# ---------- SUCCESS EXIT ----------
def exit_success(audience_count):
    result = {
        "IsSuccess": 1,
        "AudienceCount": int(audience_count),
        "ErrorDetails": ""
    }
    print("SUCCESS:", result)
    mssparkutils.notebook.exit(json.dumps(result))


# ---------- FAILURE EXIT ----------
def exit_failure(step, err):

    error_message = ""

    try:
        #  Spark SQL Syntax Error (ParseException)
        if hasattr(err, "desc") and err.desc:
            error_message = err.desc

        # Spark Analysis Error (wrong column/table)
        elif hasattr(err, "java_exception") and err.java_exception:
            error_message = err.java_exception.getMessage()

        #  Py4J Java bridge errors (ADLS, Delta, permissions)
        elif "Py4JJavaError" in str(type(err)):
            error_message = str(err)
            if "An error occurred while calling" in error_message:
                # extract the real Java cause
                parts = error_message.split(":")
                error_message = parts[-1]

        #  Normal Python error
        else:
            error_message = str(err)

    except Exception:
        error_message = str(err)

    # Clean Spark noise
    if error_message:
        error_message = error_message.split("\n")[0]
        error_message = error_message.replace("org.apache.spark.sql.AnalysisException:", "")
        error_message = error_message.replace("java.lang.RuntimeException:", "")
        error_message = error_message.replace("org.apache.hadoop.fs.azurebfs.contracts.exceptions.", "")

    result = {
        "IsSuccess": 0,
        "AudienceCount": 0,
        "ErrorDetails": f"{step}: {error_message}"
    }

    mssparkutils.notebook.exit(json.dumps(result))

-----------------------------------------------------

def run_segmentation_pipeline():

    # ---------------------------
    # STORAGE AUTHENTICATION
    # ---------------------------
    try:
        inputProps = TokenLibrary.getPropertiesAll(InputLinkedService)

        inputStorageAccountName = (
            json.loads(inputProps)["Endpoint"]
            .replace("https://","")
            .replace(".dfs.core.windows.net/","")
        )

        # Linked service mapping
        spark.conf.set("spark.storage.synapse.linkedServiceName", InputLinkedService)

        spark.conf.set(
            f"spark.storage.synapse.{inputStorageAccountName}.dfs.core.windows.net.linkedServiceName",
            InputLinkedService
        )

        spark.conf.set(
            f"fs.azure.account.oauth.provider.type.{inputStorageAccountName}.dfs.core.windows.net",
            "com.microsoft.azure.synapse.tokenlibrary.LinkedServiceBasedTokenProvider"
        )

        print("ADLS Config Prepared")

        profileContainer = "commercialmarketing"
        profileFolder = "dataproducts/connectedmarketingdata/v1/standard/"

        cdpContainer = "cdp"
        cdpFolder = "dataproducts/ucmp/v1/standard/"

        profilePath = f"abfss://{profileContainer}@{inputStorageAccountName}.dfs.core.windows.net/{profileFolder}"
        cdpPath = f"abfss://{cdpContainer}@{inputStorageAccountName}.dfs.core.windows.net/{cdpFolder}"

        print("ADLS Authentication Successful")

    except Exception as e:
        exit_failure("STORAGE AUTHENTICATION FAILED", e)

    # ---------------------------
    # LOAD DELTA TABLES
    # ---------------------------
    try:
        print("STEP 2: Loading Source Tables")

        contact_df = spark.read.format("delta").load(profilePath + "contactprofile")
        contact_df.createOrReplaceTempView("contactprofile")

        account_df = spark.read.format("delta").load(profilePath + "accountprofile")
        account_df.createOrReplaceTempView("accountprofile")

        lead_df = spark.read.format("delta").load(cdpPath + "leadattributechangerestate")
        lead_df.createOrReplaceTempView("leadattributechangerestate")


    except Exception as e:
        exit_failure("DELTA TABLE LOAD FAILED", e)


    # ---------------------------
    # SEGMENTATION QUERY
    # ---------------------------
    try:
        print("STEP 3: Running Segmentation Query")

        # 🔴 PUT YOUR SQL QUERY HERE
        df = spark.sql(segmentation_query)

        # force Spark execution
        df.limit(1).collect()

    except Exception as e:
        exit_failure("SEGMENT QUERY FAILED", e)

    # ---------------------------
    # WRITE SEGMENT TABLE
    # ---------------------------
    try:
        print("STEP 4: Writing Segment Table")
    
        abfs_path = "abfss://ucmpsegmentexport@upssynapsedev.dfs.core.windows.net/Table"
    
        spark.sql("CREATE SCHEMA IF NOT EXISTS Segmentation")
    
        segment_id_clean = segment_id.lower().replace("-", "_")
    
        result_table = f"Segmentation.segment_{segment_id_clean}"
    
        (
            df.write
            .format("delta")
            .mode("overwrite")
            .option("path", abfs_path)
            .saveAsTable(result_table)
        )
    
        print("Table created:", result_table)
    
    except Exception as e:
        exit_failure("SEGMENT TABLE WRITE FAILED", e)

    # ---------------------------
    # AUDIENCE COUNT
    # ---------------------------
    try:
        print("STEP 5: Counting Audience")
    
        audience_count = spark.sql(
            f"SELECT COUNT(*) AS cnt FROM {result_table}"
        ).collect()[0]["cnt"]
    
    except Exception as e:
        exit_failure("AUDIENCE COUNT FAILED", e)

return audience_count

-----------------------------------------------------------
try:
    audience_count = run_segmentation_pipeline()
    exit_success(audience_count)

except Exception as e:

    # Synapse throws a Py4JJavaError after notebook.exit()
    msg = str(e)

    # If our JSON already exists in the exception → notebook already exited correctly
    if '"IsSuccess"' in msg:
        raise

    # Only real unexpected python errors reach here
    exit_failure("UNEXPECTED NOTEBOOK FAILURE", e)

