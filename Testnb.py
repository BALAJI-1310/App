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
    msg = str(err).split("\n")[0]

    result = {
        "IsSuccess": 0,
        "AudienceCount": 0,
        "ErrorDetails": f"{step}: {msg}"
    }

    print("FAILED:", result)
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
        contact_df.limit(1).collect()

        account_df = spark.read.format("delta").load(profilePath + "accountprofile")
        account_df.limit(1).collect()

        lead_df = spark.read.format("delta").load(cdpPath + "leadattributechangerestate")
        lead_df.limit(1).collect()

        contact_df.createOrReplaceTempView("contactprofile")
        account_df.createOrReplaceTempView("accountprofile")
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
    # AUDIENCE COUNT
    # ---------------------------
    try:
        print("STEP 4: Counting Audience")

        audience_count = df.count()

    except Exception as e:
        exit_failure("AUDIENCE COUNT FAILED", e)


    # ---------------------------
    # WRITE SEGMENT TABLE
    # ---------------------------
    try:
        print("STEP 5: Writing Segment Table")

        spark.sql("CREATE SCHEMA IF NOT EXISTS Segmentation")

        segment_id_clean = segment_id.lower().replace(" ", "_")
        result_table = f"Segmentation.segment_{segment_id_clean}"

        df.write.mode("overwrite").saveAsTable(result_table)

        # validate write
        spark.sql(f"SELECT COUNT(*) FROM {result_table}").collect()

    except Exception as e:
        exit_failure("SEGMENT TABLE WRITE FAILED", e)


    # RETURN SUCCESS VALUE
    return audience_count
-----------------------------------------------------------

try:
    audience_count = run_segmentation_pipeline()
    exit_success(audience_count)

except Exception as e:
    # unexpected python error
    exit_failure("UNEXPECTED NOTEBOOK FAILURE", e)
