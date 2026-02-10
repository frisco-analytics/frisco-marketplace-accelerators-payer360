# Databricks notebook source
# MAGIC %md
# MAGIC ## Purpose
# MAGIC
# MAGIC This notebook identifies potential duplicate records within your data using vector similarity search.
# MAGIC
# MAGIC ## Prerequisites
# MAGIC
# MAGIC *   A Databricks workspace with access to Databricks Vector Search.
# MAGIC *   A Model Serving endpoint serving an embedding model (e.g., `databricks-gte-large-en`).
# MAGIC *   Bronze tables with the data you want to analyze, with appropriately defined `primaryKey`
# MAGIC
# MAGIC ## Outputs
# MAGIC
# MAGIC *   Creates temporary views named `<entity_name>_duplicate_candidates` for each entity in the provided data model. These views contain potential duplicate records and their similarity scores.
# MAGIC
# MAGIC This is your first step in identifying and deduplicating your data!  Run this first to create the datasets that tell you your duplicate candidates

# COMMAND ----------

# MAGIC %pip install databricks-vectorsearch
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Importing necessary libraries

# COMMAND ----------

import time
import pandas as pd
from pyspark.sql import Row
from databricks.vector_search.reranker import DatabricksReranker
from databricks.vector_search.client import VectorSearchClient
from pyspark.sql.functions import concat_ws,col
from pyspark.sql.utils import AnalysisException  # For handling table not found errors

# COMMAND ----------

# MAGIC %md
# MAGIC #### Setting Model and Search Parameters
# MAGIC These widgets will be used for searching. You will need to tune the *num_results* parameter in this model. If the value is too low there may be records with a similarity of 1.0. The most common approach for fixing this is simply adding more results.

# COMMAND ----------

dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("table_name", "Table Name")  
dbutils.widgets.text("primary_key", "", "Primary Key")
dbutils.widgets.text("columns_to_exclude", "", "Columns to exclude")
dbutils.widgets.text("embedding_model_endpoint_name","databricks-gte-large-en")
dbutils.widgets.text("num_results","3")
dbutils.widgets.text("vector_search","lakefusion_vs_endpoint","Vector Search Name")

# COMMAND ----------

embedding_model_endpoint_name = dbutils.widgets.get("embedding_model_endpoint_name")
num_results = int(dbutils.widgets.get("num_results"))
table_name = dbutils.widgets.get("table_name")
primary_key = dbutils.widgets.get("primary_key")
columns_to_exclude = dbutils.widgets.get("columns_to_exclude")
vector_search = dbutils.widgets.get("vector_search")
catalog_name = dbutils.widgets.get("catalog_name")
if not catalog_name:
    raise Exception("Catalog name is required to run this notebook")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Creating function *create_embedding_source*
# MAGIC This function is used to combine all the columns into a single data set that is then transformed into a vector. This makes it much easier to evaluate each record and makes it possible to then evaluate duplicate candidates.

# COMMAND ----------

def create_embedding_source(source_df, columns_to_exclude):
    """
    Creates the combined text column used for embedding.

    Args:
        source_df: The source DataFrame.
        columns_to_exclude: A list of columns to exclude from the combined text.

    Returns:
        A DataFrame with the 'combined_data' column added, or None if source_df is None.
    """
    if source_df is None:
        print("Warning: create_embedding_source received a None DataFrame. Returning None.")
        return None, None

    columns_to_include = [col for col in source_df.columns if col not in columns_to_exclude]
    embeddingColumn = "combined_data"  # Fixed embedding column name
    sourceDF = source_df.withColumn(embeddingColumn, concat_ws(",", *columns_to_include))
    return sourceDF, embeddingColumn

# COMMAND ----------

# MAGIC %md
# MAGIC #### Creating *create_or_get_index*
# MAGIC This model is used to build the vector index, or get it if it is already available. This model takes in the entity's data and converts each of those records into vectors. These vectors can then be efficiently searched against to allow us to find duplicate candidates.
# MAGIC *NOTE:* This model only provides candidates. These candidates are not necessarily duplicates, but can be analyzed in the next model. You will need to tune *num_results* in this model so a sufficient number of records are returned.
# MAGIC
# MAGIC This function builds the vector index if the vector index does not yet exist.

# COMMAND ----------

vsc = VectorSearchClient(disable_notice=True)

# COMMAND ----------

def create_or_get_index(table_name, embedding_model_endpoint_name, embeddingColumn, primary_key, vectorSearchEndpointName=vector_search):
    """
    Creates or retrieves the Vector Search index.

    Args:
        table_name: The name of the table to index.
        embedding_model_endpoint_name: The name of the embedding model endpoint.
        embeddingColumn: The name of the column to use for embedding.
        primary_key: The name of the primary key column.
        vectorSearchEndpointName: The name of the vector search endpoint

    Returns:
        The Vector Search index object and its name.
    """
    indexName = f"{table_name}_index"
    index = None # Ensure index is initially None in case of errors

    try:
        index = vsc.create_delta_sync_index(
            index_name=indexName,
            endpoint_name=vectorSearchEndpointName,
            source_table_name=table_name,
            embedding_source_column=embeddingColumn,
            primary_key=primary_key,
            embedding_model_endpoint_name=embedding_model_endpoint_name,
            pipeline_type="TRIGGERED"
        )
        timeout = time.time() + 1200   # set timeout for 20 minutes from now

        while not index.describe().get('status').get('detailed_state').startswith('ONLINE'):
            print("Waiting for index to be ONLINE...")
            time.sleep(30)
            if time.time() > timeout:
                print("Timeout: Index did not become ONLINE within 10 minutes.")
                raise TimeoutError("Index creation timed out.")  # Raise a TimeoutError
        print("Index is ONLINE")

    except Exception as e:
        if "RESOURCE_ALREADY_EXISTS" in str(e):
            index = vsc.get_index(endpoint_name=vectorSearchEndpointName, index_name=indexName)
            index.sync()
            while not index.describe().get('status').get('detailed_state').startswith('ONLINE_NO_PENDING_UPDATE'):
                print("Wait : latest_version_currently_processing")
                time.sleep(60)
        else:
            print(f"An unexpected error occurred during index creation: {e}")
            raise # Re-raise the exception to stop execution
    finally:
        # Optionally clean up resources (uncomment to enable)
        # try:
        #     vsc = VectorSearchClient()
        #     vsc.delete_index(indexName)
        #     print(f"Deleted index {indexName}")
        # except Exception as e:
        #     print(f"Could not delete index {indexName}: {e}")
        pass
    return index, indexName

# COMMAND ----------

# MAGIC %md
# MAGIC #### Creating *execute_similarity_search*
# MAGIC This is where all the action takes place. In this method, each of the records in the incoming tables are compared to all the records stored in the vectordb. Each of those matches are scored, and the *num_results* number of those records are returned.

# COMMAND ----------

def execute_similarity_search(sourceDF, embeddingColumn, primary_key, indexName, num_results, 
                               endpoint_name=vector_search, columns_to_rerank=None, query_type="HYBRID"):
    """
    Executes the similarity search with reranker and returns potential duplicate records.
    
    Args:
        sourceDF: The source DataFrame.
        embeddingColumn: The name of the column to use for embedding/query.
        primary_key: The name of the primary key column.
        indexName: The name of the index to search.
        num_results: The number of potential duplicates to return.
        endpoint_name: The vector search endpoint name.
        columns_to_rerank: List of columns to use for reranking (default: [embeddingColumn]).
        query_type: Query type - "ANN" or "HYBRID" (default: "HYBRID").
    
    Returns:
        A DataFrame containing potential duplicate records and their similarity scores.
    """
    
    if sourceDF is None:
        print("Warning: execute_similarity_search received a None DataFrame. Returning None.")
        return None
    
    # Initialize Vector Search Client
    index = vsc.get_index(endpoint_name=endpoint_name, index_name=indexName)
    
    # Set default columns_to_rerank if not provided
    if columns_to_rerank is None:
        columns_to_rerank = [embeddingColumn]
    
    # Collect source data (use this approach for smaller datasets)
    # For large datasets, consider batch processing or using the SQL approach
    source_data = sourceDF.select(primary_key, embeddingColumn).collect()
    
    all_results = []
    
    for row in source_data:
        original_id = row[primary_key]
        query_text = row[embeddingColumn]
        
        # Execute similarity search with reranker
        results = index.similarity_search(
            query_text=query_text,
            columns=[primary_key, embeddingColumn],
            num_results=num_results + 1,  # +1 to account for self-match
            query_type=query_type
        )
        
        # Process results and exclude self-matches
        if results and 'result' in results and 'data_array' in results['result']:
            for result_row in results['result']['data_array']:
                potential_duplicate_id = result_row[0]  # primary_key
                potential_duplicate_text = result_row[1]  # embeddingColumn
                search_score = result_row[-1]  # score is last element
                
                # Exclude self-matches
                if original_id != potential_duplicate_id:
                    all_results.append({
                        'original_id': original_id,
                        'original_combined_data': query_text,
                        'potential_duplicate_id': potential_duplicate_id,
                        'potential_duplicate_combined_data': potential_duplicate_text,
                        'search_score': search_score
                    })
    
    # Convert results to DataFrame
    if all_results:
        result_df = spark.createDataFrame(all_results)
        # Keep only top num_results per original_id
        from pyspark.sql.window import Window
        from pyspark.sql.functions import row_number
        
        window_spec = Window.partitionBy("original_id").orderBy(col("search_score").desc())
        result_df = result_df.withColumn("rank", row_number().over(window_spec)) \
                             .filter(col("rank") <= num_results) \
                             .drop("rank")
        
        return result_df
    else:
        return None


# COMMAND ----------

# MAGIC %md
# MAGIC #### Creating *detect_duplicates*
# MAGIC The overarching controller function that executes the other functions. It will take in the table name, it's primaryKey, and a list of any columns you might want to exclude.

# COMMAND ----------

def detect_duplicates(table_name, primary_key, embedding_model_endpoint_name, columns_to_exclude, num_results, vectorSearchEndpointName=vector_search):
    """
    Detects potential duplicate records within a table using vector similarity search.

    Args:
        table_name: The name of the table to deduplicate.
        primary_key: The name of the primary key column.
        embedding_model_endpoint_name: The name of the embedding model endpoint.
        columns_to_exclude: Columns to exclude from combined data.
        num_results: The number of potential duplicates to return.
        vectorSearchEndpointName: The Vector Search Endpoint (default "lakefusion_vs_endpoint")

    Returns:
        A DataFrame containing potential duplicates and their similarity scores. Returns None if an error occurs.
    """

    try:
        source_df = spark.table(table_name)
    except AnalysisException as e:
        print(f"Error: Table '{table_name}' not found: {e}")
        return None

    sourceDF, embeddingColumn = create_embedding_source(source_df, columns_to_exclude)
    catalog_name = table_name.split(".")[0]
    tableName = table_name.split(".")[-1]
    silver_table_name = f"{catalog_name}.silver.{tableName}"
    sourceDF.write.mode("overwrite").saveAsTable(silver_table_name)
    print(f"Creating silver table CDF :{silver_table_name}")
    spark.sql(f"ALTER TABLE {silver_table_name} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
    print(f"Created silver table CDF :{silver_table_name}")

    # Check if create_embedding_source returned None
    if sourceDF is None:
        return None

    index, indexName = create_or_get_index(silver_table_name, embedding_model_endpoint_name, embeddingColumn, primary_key, vectorSearchEndpointName)

    # Check if create_or_get_index returned None
    if index is None:
        return None

    duplicate_candidate_df = execute_similarity_search(sourceDF, embeddingColumn, primary_key, indexName, num_results)

    return duplicate_candidate_df

# COMMAND ----------

# MAGIC %md
# MAGIC #### Main Execution Loop
# MAGIC
# MAGIC Define the data model and run it across each entity defined.
# MAGIC
# MAGIC You will need to define the table names you want to analyze here, including the correct catalog and scheme. You will also want to ensure the correct primaryKey is specified.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Executing the model
# MAGIC For each entity, we will be executing the model and creating the candidate views.

# COMMAND ----------

print(f"Processing table: {table_name}")
duplicate_candidates = detect_duplicates(table_name, primary_key, embedding_model_endpoint_name,columns_to_exclude, num_results)
if duplicate_candidates is not None: #Check for returns from issues
    # duplicate_candidates.createOrReplaceTempView(f"{table_name.split('.')[-1]}_duplicate_candidates")
    duplicate_candidates.write.mode("overwrite").saveAsTable(f"{table_name}_duplicate_candidates")
else:
    print(f"Skipping {table_name} due to error in detect_duplicates function.")
