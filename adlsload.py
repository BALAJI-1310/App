spark.conf.set("spark.storage.synapse.linkedServiceName", InputLinkedService)

spark.conf.set(
    f"spark.storage.synapse.{inputStorageAccountName}.dfs.core.windows.net.linkedServiceName",
    InputLinkedService
)

spark.conf.set(
    f"fs.azure.account.oauth.provider.type.{inputStorageAccountName}.dfs.core.windows.net",
    "com.microsoft.azure.synapse.tokenlibrary.LinkedServiceBasedTokenProvider"
)

print("ADLS Authentication Successful")

---------------------------



profileContainer = "commercialmarketing"
profileFolder = "dataproducts/connectedmarketingdata/v1/standard/"

cdpContainer = "cdp"
cdpFolder = "dataproducts/ucmp/v1/standard/"

profilePath = f"abfss://{profileContainer}@{inputStorageAccountName}.dfs.core.windows.net/{profileFolder}"
cdpPath = f"abfss://{cdpContainer}@{inputStorageAccountName}.dfs.core.windows.net/{cdpFolder}"

print(profilePath)
print(cdpPath)

----------------------------


# Contact Profile
contact_df = spark.read.format("delta").load(profilePath + "contactprofile")
contact_df.createOrReplaceTempView("contactprofile")

# Account Profile
account_df = spark.read.format("delta").load(profilePath + "accountprofile")
account_df.createOrReplaceTempView("accountprofile")

# Lead Attribute Change Restate
lead_df = spark.read.format("delta").load(cdpPath + "leadattributechange_restate")
lead_df.createOrReplaceTempView("leadattributechangerestate")

print("CDP Tables Loaded Successfully")

---------------------------------

# Execute query
df = spark.sql(query)

print("Preview:")
display(df)



