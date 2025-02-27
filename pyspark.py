import uuid

from fastapi import FastAPI, HTTPException, APIRouter
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, explode, split, count, when, sum, collect_list, struct, sum as sql_sum, first
# from app.database.database_connection import create_cassandra_session
from app.database.database_connection import create_cassandra_session
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from pyspark.sql import functions as F
import os
import sys
from uuid import UUID

database_username = 'your-db-username'
database_password = 'your-db-password/'
database_address = 'your-db-address'
session = create_cassandra_session()

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Sconti") \
    .config("spark.cassandra.connection.host", database_address) \
    .getOrCreate()

# Create FastAPI app
analysis_router = APIRouter()


def get_rdd_values():
    print('camehere')
    filters = "user_id = 813b6e33-91ff-49f0-8e08-2f479c95e4e1 ALLOW FILTERING"
    df = (spark.read
          .format("org.apache.spark.sql.cassandra")
          .option('spark.cassandra.connection.host', database_address)
          .option('spark.cassandra.connection.port', 9042)
          .option("user", database_username)
          .option("password", database_password)
          .option("keyspace", "scontiaispace")
          .option("table", "test_table_new")
          .load()
          .where(filters))
    print(df.show())
    return df


def analyze_data(data: dict):
    df = spark.createDataFrame(data)
    # Convert is_response_correct to boolean
    df = df.withColumn("is_response_correct", col("is_response_correct").cast("boolean"))

    # Split and explode the response_categories column
    df_exp = df.withColumn("response_category", explode(split(col("response_categories"), ", ")))

    # Calculate overall counts and correct counts for each response category
    overall_counts = df_exp.groupBy("response_category") \
        .agg(
        count("*").alias("total_count"),
        sum(when(col("is_response_correct"), 1).otherwise(0)).alias("correct_count")
    )

    overall_percentages = overall_counts.select(
        "response_category",
        (col("correct_count") / col("total_count")).alias("percentage")
    ).collect()

    return overall_percentages


# def analyze_data(data: dict):
#     df = spark.createDataFrame(data)
#     # Split and explode the response_categories column
#     df_exp = df.withColumn("response_category", explode(split(col("response_categories"), ", ")))
#
#     # Calculate overall counts and correct counts for each response category
#     overall_counts = df_exp.groupBy("response_category") \
#         .agg(
#         count("*").alias("total_count"),
#         sum(when(col("is_response_correct") == True, 1).otherwise(0)).alias("correct_count")
#     )
#
#     overall_percentages = overall_counts.select(
#         "response_category",
#         (col("correct_count") / col("total_count")).alias("percentage")
#     ).collect()
#
#     return overall_percentages


# Define FastAPI endpoint
@analysis_router.post("/analysis")
def get_analysis(data:dict):
    try:
        overall_percentages = analyze_data(data['data'])
        response_object = {"analysis": [{"response_category": row["response_category"],
                                         "percentage": row["percentage"]} for row in overall_percentages]}
        # response_object = {"analysis":get_rdd_values()}
        return response_object
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


def get_responses_of_user(user_id: str, exam_id:str):
    exam_id_uuid = UUID(exam_id)
    user_responses_fetch_query = """SELECT * FROM scontiaispace.user_responses_new WHERE user_id=%s AND exam_ids CONTAINS %s ALLOW FILTERING; """
    test_analysis = session.execute(user_responses_fetch_query, (user_id,exam_id_uuid))
    test_analysis_list = []
    test_analysis_analysis = test_analysis.all()
    index_str = "0"
    for analysis in test_analysis_analysis:
        test_id_str = str(analysis.test_id) if isinstance(analysis.test_id, UUID) else analysis.test_id
        test_analysis_list.append({
            "test_id": test_id_str,
            "test_name": analysis.test_name,
            "subject_id": analysis.subject_ids._items[0],
            "subject_name": analysis.subject_name,
            "correct_answer": analysis.correct_option,
            "user_response": analysis.user_response,
            "topic_id": analysis.topic_ids._items[0],
            "topic_name": analysis.topic_name

        })
    print(test_analysis_list)
    print(len(test_analysis_list))
    return test_analysis_list

# def get_responses_of_user_analysis_table(user_id: str, subject_id: str):
#     subject_id_uuid = uuid.UUID(subject_id)
#     user_responses_fetch_query = """SELECT * FROM scontiaispace.user_test_data_by_exam_and_subject
#     WHERE user_id=%s AND subject_ids CONTAINS %s ALLOW FILTERING"""
#     test_analysis = session.execute(user_responses_fetch_query, (user_id, subject_id_uuid))
#     test_analysis_list = []
#     test_analysis_analysis = test_analysis.all()
#     index_str = "0"
#     for analysis in test_analysis_analysis:
#         test_id_str = str(analysis.test_id) if isinstance(analysis.test_id, UUID) else analysis.test_id
#         test_analysis_list.append({
#             "test_id": test_id_str,
#             "test_name": analysis.test_name,
#             "subject_id": analysis.subject_ids._items[0],
#             "subject_name": analysis.subject_name,
#             "test_score": analysis.test_score,
#             "topic_id": analysis.topic_ids._items[0],
#             "topic_name": analysis.topic_name,
#             "correct_answer_count": analysis.correct_answer_count,
#             "wrong_answer_count": analysis.wrong_answer_count,
#             "skipped_answer_count": analysis.skipped_answer_count
#
#         })
#     print(test_analysis_list)
#     print(len(test_analysis_list))
#     return test_analysis_list

def get_responses_of_user_analysis_table(user_id: str, subject_id: str, exam_id: str):
    subject_id_uuid = UUID(subject_id)
    exam_id_uuid = UUID(exam_id)
    user_responses_fetch_query = """SELECT * FROM scontiaispace.user_test_data_by_exam_and_subject 
    WHERE user_id=%s AND exam_ids CONTAINS %s AND subject_ids CONTAINS %s ORDER BY created_date ASC ALLOW FILTERING"""
    test_analysis = session.execute(user_responses_fetch_query, (user_id,exam_id_uuid, subject_id_uuid))
    test_analysis_list = []
    test_analysis_analysis = test_analysis.all()
    index_str = "0"
    for analysis in test_analysis_analysis:
        test_id_str = str(analysis.test_id) if isinstance(analysis.test_id, UUID) else analysis.test_id
        correct_count = analysis.correct_answer_count if hasattr(analysis, 'correct_answer_count') else 0
        wrong_count = analysis.wrong_answer_count if hasattr(analysis, 'wrong_answer_count') else 0
        skipped_count = analysis.skipped_answer_count if hasattr(analysis, 'skipped_answer_count') else 0
        test_analysis_list.append({
            "test_id": test_id_str,
            "test_name": analysis.test_name,
            "subject_id": analysis.subject_ids._items[0],
            "subject_name": analysis.subject_name,
            "test_score": analysis.test_score,
            "topic_id": analysis.topic_ids._items[0],
            "topic_name": analysis.topic_name,
            "correct_answer_count": correct_count,
            "wrong_answer_count": wrong_count,
            "skipped_answer_count": skipped_count
        })
    print(test_analysis_list)
    print(len(test_analysis_list))
    return test_analysis_list
def get_subject_id_count(data, spark_session):
    # Define the schema for the DataFrame
    schema = StructType([
        StructField("response_id", StringType(), True),
        StructField("test_id", StringType(), True),
        StructField("test_name", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("user_name", StringType(), True),
        StructField("exam_id", StringType(), True),  # Specify StringType for exam_id
        StructField("exam_name", StringType(), True),
        StructField("subject_id", StringType(), True),
        StructField("subject_name", StringType(), True),
        StructField("topic_id", StringType(), True),
        StructField("topic_name", StringType(), True),
        StructField("sub_topic_id", StringType(), True),
        StructField("sub_topic_name", StringType(), True),
        StructField("difficulty_level", StringType(), True),
        StructField("question_id", StringType(), True),
        StructField("question_number", StringType(), True),
        StructField("categories", StringType(), True),
        StructField("question_text", StringType(), True),
        StructField("option_a", StringType(), True),
        StructField("option_b", StringType(), True),
        StructField("option_c", StringType(), True),
        StructField("option_d", StringType(), True),
        StructField("correct_answer", StringType(), True),
        StructField("user_response", StringType(), True),
        StructField("is_response_correct", StringType(), True),
        StructField("explanation", StringType(), True),
    ])

    # Create a DataFrame with the specified schema
    df = spark_session.createDataFrame(data, schema=schema)

    # Group by 'subject_id' and count the occurrences
    subject_count_df = df.groupBy("subject_id").count()

    # Collect the result as a list of objects
    result_list = subject_count_df.collect()

    # Convert the result list to a list of dictionaries
    result_dict_list = [item.asDict() for item in result_list]

    return result_dict_list


def get_subject_test_counts_unique(data, spark_session):
    # Define the schema for the DataFrame
    schema = StructType([
        StructField("response_id", StringType(), True),
        StructField("test_id", StringType(), True),
        StructField("test_name", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("user_name", StringType(), True),
        StructField("exam_id", StringType(), True),
        StructField("exam_name", StringType(), True),
        StructField("subject_id", StringType(), True),
        StructField("subject_name", StringType(), True),
        StructField("topic_id", StringType(), True),
        StructField("topic_name", StringType(), True),
        StructField("sub_topic_id", StringType(), True),
        StructField("sub_topic_name", StringType(), True),
        StructField("difficulty_level", StringType(), True),
        StructField("question_id", StringType(), True),
        StructField("question_number", StringType(), True),
        StructField("categories", StringType(), True),
        StructField("question_text", StringType(), True),
        StructField("option_a", StringType(), True),
        StructField("option_b", StringType(), True),
        StructField("option_c", StringType(), True),
        StructField("option_d", StringType(), True),
        StructField("correct_answer", StringType(), True),
        StructField("user_response", StringType(), True),
        StructField("is_response_correct", StringType(), True),
        StructField("explanation", StringType(), True),
    ])

    # Create a DataFrame with the specified schema
    df = spark_session.createDataFrame(data, schema=schema)

    # Get unique subject_id values from the data
    unique_subject_ids = df.select("subject_id").distinct().collect()

    result_list = []

    for row in unique_subject_ids:
        subject_id = row["subject_id"]

        # Filter the DataFrame to include only the current subject_id
        filtered_df = df.filter(df.subject_id == subject_id)

        # Group by 'test_id' and count the occurrences
        test_count_df = filtered_df.groupBy("test_id").count()

        # Collect the result as a list of dictionaries
        test_count_list = test_count_df.collect()

        # Convert the result list to a list of dictionaries
        test_count_dict_list = [item.asDict() for item in test_count_list]

        # Append the subject_id and test count to the result list
        result_list.append(
            {"subject_id": subject_id, "test_count": len(test_count_dict_list), "test_details": test_count_dict_list})

    return result_list


def get_subject_test_counts(data, spark_session):
    # Define the schema for the DataFrame
    schema = StructType([
        StructField("response_id", StringType(), True),
        StructField("test_id", StringType(), True),
        StructField("test_name", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("user_name", StringType(), True),
        StructField("exam_id", StringType(), True),
        StructField("exam_name", StringType(), True),
        StructField("subject_id", StringType(), True),
        StructField("subject_name", StringType(), True),
        StructField("topic_id", StringType(), True),
        StructField("topic_name", StringType(), True),
        StructField("sub_topic_id", StringType(), True),
        StructField("sub_topic_name", StringType(), True),
        StructField("difficulty_level", StringType(), True),
        StructField("question_id", StringType(), True),
        StructField("question_number", StringType(), True),
        StructField("categories", StringType(), True),
        StructField("question_text", StringType(), True),
        StructField("option_a", StringType(), True),
        StructField("option_b", StringType(), True),
        StructField("option_c", StringType(), True),
        StructField("option_d", StringType(), True),
        StructField("correct_answer", StringType(), True),
        StructField("user_response", StringType(), True),
        StructField("is_response_correct", StringType(), True),
        StructField("explanation", StringType(), True),
    ])

    # Create a DataFrame with the specified schema
    df = spark_session.createDataFrame(data, schema=schema)

    # Group by 'subject_id', 'test_id', and 'subject_name', and count the occurrences
    subject_test_count_df = df.groupBy("subject_id", "test_id", "subject_name").count()

    # Collect the result as a list of dictionaries
    result_list = subject_test_count_df.collect()

    # Convert the result list to a list of dictionaries
    result_dict_list = [item.asDict() for item in result_list]

    return result_dict_list


# def get_subject_test_counts_name(data, spark_session):
#     # Define the schema for the DataFrame
#     schema = StructType([
#         StructField("test_id", StringType(), True),
#         StructField("test_name", StringType(), True),
#         StructField("subject_id", StringType(), True),
#         StructField("subject_name", StringType(), True),
#         StructField("correct_answer", StringType(), True),
#         StructField("user_response", StringType(), True),
#     ])
#
#     # Create a DataFrame with the specified schema
#     df = spark_session.createDataFrame(data, schema=schema)
#
#     # Get unique subject_id values along with their corresponding subject_name
#     unique_subjects = df.select("subject_id", "subject_name").distinct().collect()
#
#     result_list = []
#
#     for row in unique_subjects:
#         subject_id = row["subject_id"]
#         subject_name = row["subject_name"]
#
#         # Filter the DataFrame to include only the current subject_id
#         filtered_df = df.filter(df.subject_id == subject_id)
#
#         # Group by 'test_id' and count the occurrences
#         test_count_df = filtered_df.groupBy("test_id").count()
#
#         # Collect the result as a list of dictionaries
#         test_count_list = test_count_df.collect()
#
#         # Convert the result list to a list of dictionaries
#         test_count_dict_list = [item.asDict() for item in test_count_list]
#
#         # Append the subject_id, subject_name, and test count to the result list
#         result_list.append({
#             "subject_id": subject_id,
#             "subject_name": subject_name,
#             "test_count": len(test_count_dict_list),
#             "test_details": test_count_dict_list
#         })
#
#     return result_list

def get_subject_test_counts_name(data, spark_session):
    # Define the schema for the DataFrame
    schema = StructType([
        StructField("test_id", StringType(), True),
        StructField("test_name", StringType(), True),
        StructField("subject_id", StringType(), True),
        StructField("subject_name", StringType(), True),
        StructField("correct_answer", StringType(), True),
        StructField("user_response", StringType(), True),
    ])

    # Create a DataFrame with the specified schema
    df = spark_session.createDataFrame(data, schema=schema)

    # Group by 'subject_id' and 'subject_name', then count distinct 'test_id' within each group
    result_df = df.groupBy("subject_id", "subject_name") \
        .agg(F.countDistinct("test_id").alias("test_count"), F.collect_list(F.struct("test_id", "test_name")).alias("test_details"))

    # Collect the result DataFrame into a list of dictionaries
    result_list = result_df.rdd.map(lambda row: {
        "subject_id": row["subject_id"],
        "subject_name": row["subject_name"],
        "test_count": row["test_count"],
        # "test_details": [{"test_id": detail["test_id"], "test_name": detail["test_name"]} for detail in row["test_details"]]
    }).collect()

    return result_list
@analysis_router.get("/analysis_subject")
def get_analysis(user_id : str, exam_id: str):
    try:
        overall_percentages = get_subject_test_counts_name(
            get_responses_of_user(user_id, exam_id), spark)
        # overall_percentages = get_responses_of_user("813b6e33-91ff-49f0-8e08-2f479c95e4e1")
        return {"messages": "Analysis Created Successfully", "analysis": overall_percentages}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# Run FastAPI app


def calculate_score(correct_answer, user_response):
    # Define your logic to calculate the score
    return 1 if correct_answer == user_response else 0



# def get_subject_test_scores(data, spark):
#     # Create a Spark session
#     schema = StructType([
#         StructField("test_id", StringType(), True),
#         StructField("test_name", StringType(), True),
#         StructField("subject_id", StringType(), True),
#         StructField("subject_name", StringType(), True),
#         StructField("correct_answer", StringType(), True),
#         StructField("user_response", StringType(), True),
#     ])
#
#     # Convert the input data (list of dictionaries) to a DataFrame with the specified schema
#     df = spark.createDataFrame(data, schema=schema)
#
#     # Calculate test scores using DataFrame transformations
#     test_scores_df = df.groupBy("subject_id", "subject_name", "test_id", "test_name") \
#         .agg(sum((col("correct_answer") == col("user_response")).cast("int")).alias("correct_responses_count"),
#              count("*").alias("total_questions_count"))
#
#     test_scores_df = test_scores_df.withColumn("test_score",
#                                                100 * (col("correct_responses_count") / col("total_questions_count")))
#
#     # Collect the results back to the driver node
#     result_list = test_scores_df.groupBy("subject_id", "subject_name") \
#         .agg(collect_list(struct("test_id", "test_name", "test_score")).alias("tests")) \
#         .select("subject_name", "tests") \
#         .rdd.map(lambda row: {
#         "name": row["subject_name"],
#         "type": "line",
#         "stack": "Total",
#         "data": [test["test_score"] for test in row["tests"]]
#     }).collect()
#
#     # spark.stop()  # Stop the Spark session
#
#     return {"analysis": result_list}

# def get_subject_test_scores(data, spark):
#     # Create a Spark session
#     schema = StructType([
#         StructField("test_id", StringType(), True),
#         StructField("test_name", StringType(), True),
#         StructField("subject_id", StringType(), True),
#         StructField("subject_name", StringType(), True),
#         StructField("correct_answer", StringType(), True),
#         StructField("user_response", StringType(), True),
#     ])
#
#     # Convert the input data (list of dictionaries) to a DataFrame with the specified schema
#     df = spark.createDataFrame(data, schema=schema)
#
#     # Calculate test scores using DataFrame transformations
#     test_scores_df = df.groupBy("subject_id", "subject_name", "test_id", "test_name") \
#         .agg(F.sum((F.col("correct_answer") == F.col("user_response")).cast("int")).alias("correct_responses_count"),
#              F.count("*").alias("total_questions_count"))
#
#     test_scores_df = test_scores_df.withColumn("test_score",
#                                                100 * (F.col("correct_responses_count") / F.col("total_questions_count")))
#
#     # Collect the results back to the driver node
#     result_list = test_scores_df.groupBy("subject_id", "subject_name") \
#         .agg(F.collect_list(F.struct("test_id", "test_name", "test_score")).alias("tests")) \
#         .select("subject_name", "tests") \
#         .rdd.map(lambda row: {
#         "name": row["subject_name"],
#         "type": "line",
#         "stack": "Total",
#         "data": [test["test_score"] for test in row["tests"]]
#     }).collect()
#
#     # Get distinct test names and subject names
#     test_names_list = df.select("test_name").distinct().rdd.map(lambda row: row["test_name"]).collect()
#     subject_names_list = df.select("subject_name").distinct().rdd.map(lambda row: row["subject_name"]).collect()
#
#     # Create JSON output
#     output_json = {
#         "subject_names_list": subject_names_list,
#         "test_names_list": test_names_list,
#         "analysis": result_list
#     }
#
#     return output_json

# def get_subject_test_scores(data, spark):
#     # Create a Spark session
#     schema = StructType([
#         StructField("test_id", StringType(), True),
#         StructField("test_name", StringType(), True),
#         StructField("subject_id", StringType(), True),
#         StructField("subject_name", StringType(), True),
#         StructField("correct_answer", StringType(), True),
#         StructField("user_response", StringType(), True),
#     ])
#
#     # Convert the input data (list of dictionaries) to a DataFrame with the specified schema
#     df = spark.createDataFrame(data, schema=schema)
#
#     # Calculate test scores using DataFrame transformations
#     test_scores_df = df.groupBy("subject_id", "subject_name", "test_id", "test_name") \
#         .agg(F.sum((F.col("correct_answer") == F.col("user_response")).cast("int")).alias("correct_responses_count"),
#              F.count("*").alias("total_questions_count"))
#
#     test_scores_df = test_scores_df.withColumn("test_score",
#                                                100 * (F.col("correct_responses_count") / F.col("total_questions_count")))
#
#     # Get distinct test names
#     test_names_list = df.select("test_name").distinct().rdd.map(lambda row: row["test_name"]).collect()
#
#     # Collect the results back to the driver node
#     result_list = test_scores_df.groupBy("subject_id", "subject_name") \
#         .agg(F.collect_list(F.struct("test_id", "test_name", "test_score")).alias("tests")) \
#         .select("subject_name", "tests") \
#         .rdd.map(lambda row: {
#         "name": row["subject_name"],
#         "type": "line",
#         "stack": "Total",
#         "data": [test["test_score"] for test in sorted(row["tests"], key=lambda x: test_names_list.index(x["test_name"]))]
#     }).collect()
#
#     # Create JSON output
#     output_json = {
#         "subject_names_list": sorted(df.select("subject_name").distinct().rdd.map(lambda row: row["subject_name"]).collect()),
#         "test_names_list": test_names_list,
#         "analysis": result_list
#     }
#
#     return output_json

def get_subject_test_scores(data, spark):
    # Create a Spark session
    schema = StructType([
        StructField("test_id", StringType(), True),
        StructField("test_name", StringType(), True),
        StructField("subject_id", StringType(), True),
        StructField("subject_name", StringType(), True),
        StructField("correct_answer", StringType(), True),
        StructField("user_response", StringType(), True),
    ])

    # Convert the input data (list of dictionaries) to a DataFrame with the specified schema
    df = spark.createDataFrame(data, schema=schema)

    # Calculate test scores using DataFrame transformations
    test_scores_df = df.groupBy("subject_id", "subject_name", "test_id", "test_name") \
        .agg(F.sum((F.col("correct_answer") == F.col("user_response")).cast("int")).alias("correct_responses_count"),
             F.count("*").alias("total_questions_count"))

    test_scores_df = test_scores_df.withColumn("test_score",
                                               100 * (F.col("correct_responses_count") / F.col("total_questions_count")))

    # Get distinct test names
    test_names_list = df.select("test_name").distinct().rdd.map(lambda row: row["test_name"]).collect()

    # Modify test names to desired format (e.g., test1, test2, ...)
    modified_test_names_list = [f"T{i+1}" for i in range(len(test_names_list))]

    # Collect the results back to the driver node
    result_list = test_scores_df.groupBy("subject_id", "subject_name") \
        .agg(F.collect_list(F.struct("test_id", "test_name", "test_score")).alias("tests")) \
        .select("subject_name", "tests") \
        .rdd.map(lambda row: {
        "name": row["subject_name"],
        "type": "line",
        "stack": "Total",
        "data": [test["test_score"] for test in sorted(row["tests"], key=lambda x: test_names_list.index(x["test_name"]))]
    }).collect()

    # Create JSON output with modified test names
    output_json = {
        "subject_names_list": sorted(df.select("subject_name").distinct().rdd.map(lambda row: row["subject_name"]).collect()),
        "test_names_list": modified_test_names_list,
        "analysis": result_list
    }

    return output_json

def get_subject_test_scores_newtable(data, spark):
    schema = StructType([
        StructField("test_id", StringType(), True),
        StructField("test_name", StringType(), True),
        StructField("subject_id", StringType(), True),
        StructField("subject_name", StringType(), True),
        StructField("test_score", FloatType(), True),
        StructField("correct_answer_count", IntegerType(), True),
        StructField("wrong_answer_count", IntegerType(), True),
        StructField("skipped_answer_count", IntegerType(), True),
    ])
    df = spark.createDataFrame(data, schema=schema)

    # Step 3: Extract test_name and test_score columns
    test_names = df.select("test_name").rdd.flatMap(lambda x: x).collect()
    test_scores = df.select("test_score").rdd.flatMap(lambda x: x).collect()
    correct_answer_score = df.select("correct_answer_count").rdd.flatMap(lambda x: x).collect()
    wrong_answer_score = df.select("wrong_answer_count").rdd.flatMap(lambda x: x).collect()
    skipped_answer_score = df.select("skipped_answer_count").rdd.flatMap(lambda x: x).collect()

    # Step 4: Return lists of test_name and test_score
    output_json = {
        "test_names": test_names,
        "test_scores": test_scores
    }
    correct_output_array = [{"label": "T{}".format(i + 1), "value": round(score,2)} for i, score in enumerate(correct_answer_score)]
    wrong_output_array = [{"label": "T{}".format(i + 1), "value": round(score,2)} for i, score in enumerate(wrong_answer_score)]
    skipped_output_array = [{"label": "T{}".format(i + 1), "value": round(score,2)} for i, score in enumerate(skipped_answer_score)]
    output_final = {
        "correct_answer":correct_output_array,
        "wrong_answer":wrong_output_array,
        "skipped_answer": skipped_output_array
    }
    return output_final
@analysis_router.get("/analysis_subject_score")
def get_analysis(user_id : str, subject_id:str, exam_id:str):
    try:
        # subject_id_uuid = uuid.UUID(subject_id)
        overall_percentages = get_subject_test_scores_newtable(get_responses_of_user_analysis_table(user_id, subject_id, exam_id), spark)
        # overall_percentages = get_responses_of_user("813b6e33-91ff-49f0-8e08-2f479c95e4e1")
        return {"messages": "Analysis Created Successfully", "analysis": overall_percentages}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))




def generate_heatmap_data(data: DataFrame, spark_session) -> dict:
        # Calculate score for each subject in each test
        schema = StructType([
            StructField("test_id", StringType(), True),
            StructField("test_name", StringType(), True),
            StructField("subject_id", StringType(), True),  # Assuming subject_id is of type string
            StructField("subject_name", StringType(), True),
            StructField("correct_answer", StringType(), True),
            StructField("user_response", StringType(), True),
            StructField("topic_id", StringType(), True),
            StructField("topic_name", StringType(), True)
        ])

        # Create a Spark DataFrame from the input data with the specified schema
        df = spark_session.createDataFrame(data, schema=schema)

        # Calculate score for each subject in each test
        # scores_df = df.groupBy("test_id", "test_name", "subject_id", "subject_name") \
        #     .agg(sum(when(col("user_response") == col("correct_answer"), 1).otherwise(0)).alias("score"))
        scores_df = df.groupBy("test_id", "test_name", "topic_id", "topic_name") \
            .agg(sum(when(col("user_response") == col("correct_answer"), 1).otherwise(0)).alias("score"))
        # Pivot the data to get subjects as rows and tests as columns
        # pivoted_df = scores_df.groupBy("subject_id", "subject_name") \
        #     .pivot("test_id") \
        #     .agg(first("score")) \
        #     .na.fill(0)  # Fill null values with 0
        pivoted_df = scores_df.groupBy("topic_id", "topic_name") \
            .pivot("test_id") \
            .agg(first("score")) \
            .na.fill(0)
        # Convert DataFrame to list of lists for heatmap analysis
        heatmap_data = pivoted_df.collect()
        # heatmap_data_list = [
        #     [row["subject_id"], row["subject_name"]] + [int(row[test_id]) for test_id in pivoted_df.columns[2:]] for row
        #     in heatmap_data]

        heatmap_data_list = [
            [row["topic_id"], row["topic_name"]] + [int(row[test_id]) for test_id in pivoted_df.columns[2:]] for row
            in heatmap_data]

        # Prepare the analysis format
        analysis_data = [[idx, test_idx, score] for idx, (_, subject, *scores) in enumerate(heatmap_data_list) for
                         test_idx, score in enumerate(scores)]

        # return {
        #     "analysis": analysis_data,
        #     "subject_names": [row["subject_name"] for row in heatmap_data],
        #     "test_names": pivoted_df.columns[2:],  # Exclude subject_id and subject_name columns
        # }
        return {
            "analysis": analysis_data,
            "subject_names": [row["topic_name"] for row in heatmap_data],
            "test_names": pivoted_df.columns[2:],  # Exclude subject_id and subject_name columns
        }


@analysis_router.get("/analysis_subject_test_score")
def get_analysis(user_id : str, exam_id: str):
    try:
        overall_percentages = generate_heatmap_data(get_responses_of_user(user_id, exam_id), spark)
        # overall_percentages = get_responses_of_user("813b6e33-91ff-49f0-8e08-2f479c95e4e1")
        return {"messages": "Analysis Created Successfully", "analysis": overall_percentages}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))