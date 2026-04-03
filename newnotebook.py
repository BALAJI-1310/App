# ============================================================
# CELL 1: Imports
# ============================================================

import json
import traceback
from datetime import datetime
from pyspark.sql import functions as F

print("🚀 Export Notebook Started")



# ============================================================
# CELL 2: Parameters (Injected from Pipeline)
# ============================================================

# Synapse will override this
export_config = "ABC"



# ============================================================
# CELL 3: Parse export_config
# ============================================================

def safe_json(value):
    try:
        if value and value != "null":
            return json.loads(value)
        return None
    except:
        return None

try:
    config = json.loads(export_config)

    export_id          = config.get("ExportId")
    export_name        = config.get("ExportName")
    segment_id         = config.get("SegmentId")
    export_type        = config.get("ExportType")
    watermark_column   = config.get("WatermarkColumn")
    target_source      = config.get("TargetSource")
    target_identifier  = config.get("TargetIdentifier")
    filters_json       = config.get("FiltersJson")
    filter_logic       = config.get("FilterLogic")
    filter_expression  = config.get("FilterExpression")
    column_mappings    = config.get("ColumnMappingsJson")
    static_metadata    = config.get("StaticMetadataJson")
    last_exported_at   = config.get("LastExportedAt")

    print("✅ Config parsed successfully")
    print(f"Export: {export_name}")

except Exception as e:
    print("❌ Config parsing failed")
    print(str(e))
    raise


# ============================================================
# CELL 4: Read Segment Data
# ============================================================

try:
    print("📊 Reading segment table...")

    df = spark.table(f"segmentation.segment_{segment_id}")

    print("✅ Data loaded")

except Exception as e:
    print("❌ Failed to read segment data")
    print(str(e))
    raise

# ============================================================
# CELL 5: Incremental Filter
# ============================================================

if export_type == "Incremental" and watermark_column and last_exported_at:
    print("⏳ Applying incremental filter...")
    df = df.filter(F.col(watermark_column) > F.lit(last_exported_at))


# ============================================================
# CELL 6: Apply Filters
# ============================================================

filters = safe_json(filters_json)

if filters:
    print("🔍 Applying filters...")
    conditions = []

    for f_item in filters:
        col_name = f_item.get("column")
        operator = f_item.get("operator")
        value    = f_item.get("value")

        if operator == "equals":
            conditions.append(F.col(col_name) == value)
        elif operator == "not equals":
            conditions.append(F.col(col_name) != value)
        elif operator == "contains":
            conditions.append(F.col(col_name).contains(value))
        elif operator == "starts with":
            conditions.append(F.col(col_name).startswith(value))
        elif operator == "is empty":
            conditions.append(F.col(col_name).isNull() | (F.col(col_name) == ""))
        elif operator == "is not empty":
            conditions.append(F.col(col_name).isNotNull() & (F.col(col_name) != ""))
        elif operator == "is in":
            values = [v.strip() for v in str(value).split(",")]
            conditions.append(F.col(col_name).isin(values))
        elif operator in (">", ">=", "<", "<=", "=", "!="):
            conditions.append(F.expr(f"`{col_name}` {operator} '{value}'"))
        elif operator == "between":
            vals = [v.strip() for v in str(value).split(",")]
            conditions.append(F.col(col_name).between(vals[0], vals[1]))

    if conditions:
        combined = conditions[0]
        for c in conditions[1:]:
            combined = combined & c if filter_logic != "Or" else combined | c

        df = df.filter(combined)



# ============================================================
# CELL 7: Column Mapping
# ============================================================

print("🔄 Applying column mappings...")

mappings = safe_json(column_mappings)
select_exprs = []

if mappings:
    for m in mappings:
        src = m.get("sourceColumn")
        tgt = m.get("targetColumn")
        transform = m.get("transform", "None")

        if transform == "None":
            select_exprs.append(F.col(src).alias(tgt))
        elif transform == "Trim":
            select_exprs.append(F.trim(F.col(src)).alias(tgt))
        elif transform == "Upper":
            select_exprs.append(F.upper(F.col(src)).alias(tgt))
        elif transform == "Lower":
            select_exprs.append(F.lower(F.col(src)).alias(tgt))
        elif transform == "SHA256":
            select_exprs.append(F.sha2(F.col(src).cast("string"), 256).alias(tgt))
        elif transform == "MD5":
            select_exprs.append(F.md5(F.col(src).cast("string")).alias(tgt))
        elif transform.startswith("Format:"):
            fmt = transform.split(":", 1)[1]
            select_exprs.append(F.date_format(F.col(src), fmt).alias(tgt))

# ============================================================
# CELL 8: Static Metadata
# ============================================================

statics = safe_json(static_metadata)

if statics:
    print("➕ Adding static metadata...")
    for s in statics:
        select_exprs.append(
            F.lit(s.get("staticValue")).alias(s.get("destinationColumn"))
        )

df_export = df.select(select_exprs) if select_exprs else df


# ============================================================
# CELL 9: Write CSV
# ============================================================

print("📤 Writing CSV...")

timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
blob_path = f"segment-exports/{target_source}/{target_identifier}/{export_name}_{timestamp}"

storage_account = "segmentationexports"
container = "audiences"

output_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net/{blob_path}"

df_export.coalesce(1).write \
    .option("header", "true") \
    .mode("overwrite") \
    .csv(output_path)

print(f"✅ Written to: {output_path}")


# ============================================================
# CELL 10: Return Output
# ============================================================

file_size = 0

try:
    files = dbutils.fs.ls(output_path)
    csv_files = [f for f in files if f.name.endswith(".csv")]
    if csv_files:
        file_size = csv_files[0].size
except:
    pass

result = {
    "total_records": df_export.count(),
    "file_size_bytes": file_size,
    "blob_path": blob_path
}

print("🎉 Export Completed")

dbutils.notebook.exit(json.dumps(result))
