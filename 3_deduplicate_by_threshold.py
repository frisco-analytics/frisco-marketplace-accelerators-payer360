# Databricks notebook source
# MAGIC %md
# MAGIC ## Purpose
# MAGIC
# MAGIC This notebook filters potential duplicate records based on entity-specific similarity score thresholds, creating a "gold" table.
# MAGIC
# MAGIC ## Prerequisites
# MAGIC
# MAGIC *   You must run notebook 1, as that model creates the correct Vector Search DB and the temporary candidate views.
# MAGIC *   You must run notebook 2, as that creates the data set used for model parameterization and the definition of appropriate thresholds.
# MAGIC *   Make sure the threshold parameters and the bronze table is correctly specified.
# MAGIC
# MAGIC ## Outputs
# MAGIC
# MAGIC *   Writes the filtered data for each entity to its corresponding gold table. After running this, you should see a new table with a specific model that is deduped.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### dbutils Parameters
# MAGIC The gold table is the only thing we need a dbutils parameter for here, but you will have to verify the table parameters such as catalog, database, name.

# COMMAND ----------

dbutils.widgets.text("catalog_name", "", "Catalog Name")
catalog_name = dbutils.widgets.get("catalog_name")
if not catalog_name:
    raise Exception("Catalog name is required to run this notebook")
gold_table_root = f"{catalog_name}.gold"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Model Parameters
# MAGIC You may have a number of entities to process with a certain degree of complexity. Here, you will want to set table names, model keys, and then tweak individual parameters so the model has the most accurate results. After a model runs, you can adjust these params.
# MAGIC
# MAGIC These params can be found in Notebook 1 and 2.

# COMMAND ----------

# Define the entities to process (same as Notebook 1)
entities = [
    {"table_name": f"{catalog_name}.bronze.payer", "primary_key": "payer_id", "columns_to_exclude": ["payer_id"]},
    {"table_name": f"{catalog_name}.bronze.payer_accreditation", "primary_key": "accreditation_id", "columns_to_exclude": ["accreditation_id", "payer_id"]},
    {"table_name": f"{catalog_name}.bronze.payer_address", "primary_key": "address_location_id", "columns_to_exclude": ["address_location_id", "patient_id"]},
    {"table_name": f"{catalog_name}.bronze.payer_alt_identifiers", "primary_key": "identifier_id", "columns_to_exclude": ["identifier_id", "patient_id"]},
    {"table_name": f"{catalog_name}.bronze.payer_alt_name", "primary_key": "alt_name_id", "columns_to_exclude": ["alt_name_id", "patient_id"]},
    {"table_name": f"{catalog_name}.bronze.payer_contact_information", "primary_key": "contact_id", "columns_to_exclude": ["contact_id", "patient_id"]},
    {"table_name": f"{catalog_name}.bronze.payer_network", "primary_key": "payer_network_id", "columns_to_exclude": ["payer_network_id", "patient_id"]}
    ]

# COMMAND ----------

# MAGIC %md
# MAGIC #### Model Parameters
# MAGIC You must properly specify these thresholds based on the recommendations output by Notebook 2. If it recommends .98 for payer, then make sure you have that. The gold table in this case refers to the table being built *after* the model is completed and may overwrite the bronze table, based on configuration.

# COMMAND ----------

# Define the thresholds for each entity
thresholds = {
    f"{catalog_name}.bronze.payer": 0.98,
    f"{catalog_name}.bronze.payer_accreditation": 0.97,
    f"{catalog_name}.bronze.payer_address": 0.98,
    f"{catalog_name}.bronze.payer_alt_identifiers": 0.97,
    f"{catalog_name}.bronze.payer_alt_name": 0.98,
    f"{catalog_name}.bronze.payer_contact_information": 0.98,
    f"{catalog_name}.bronze.payer_network": 1
}

# COMMAND ----------

# MAGIC %md
# MAGIC #### Process the entity
# MAGIC In this step we are applying the model to our dataset. You must verify your tables have the correct names or this will fail.

# COMMAND ----------

# Process each entity
for entity in entities:
    table_name = entity["table_name"]
    primary_key = entity["primary_key"]
    entity_name = table_name.split(".")[-1]
    candidate_view = f"{table_name}_duplicate_candidates"
    gold_table = f"{gold_table_root}.{entity_name}"
    
    if table_name not in thresholds:
        print(f"No threshold found for {table_name}. Skipping...")
        continue
    
    threshold = thresholds[table_name]
    print(f"\n{'='*60}")
    print(f"Processing: {table_name}")
    print(f"Threshold: {threshold}")
    print(f"Primary Key: {primary_key}")
    
    try:
        bronze_df = spark.table(table_name)
        candidate_df = spark.table(candidate_view)
    except Exception as e:
        print(f"Error: {e}")
        continue
    
    # CORRECTED: Using potential_duplicate_id column
    filtered_data = spark.sql(f"""
        SELECT distinct t1.*
        FROM {table_name} t1
        LEFT JOIN (
            -- Identify IDs to remove from bidirectional matches
            SELECT DISTINCT
                GREATEST(original_id, potential_duplicate_id) AS duplicate_id
            FROM {candidate_view}
            WHERE search_score >= {threshold}
        ) t2 ON t1.{primary_key} = t2.duplicate_id
        WHERE t2.duplicate_id IS NULL
    """)
    
    # Count and report
    original_count = bronze_df.count()
    filtered_count = filtered_data.count()
    removed_count = original_count - filtered_count
    
    print(f"\nResults:")
    print(f"  Removed: {removed_count}")
    
    # Write to gold table
    filtered_data.write.mode("overwrite").saveAsTable(gold_table)
    print(f"✓ Successfully wrote to {gold_table}")
    spark.sql(f"DROP TABLE IF EXISTS {candidate_view}")
