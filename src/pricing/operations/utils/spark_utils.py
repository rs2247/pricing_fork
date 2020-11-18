from pyspark.sql.types import StringType

def register_spark_methods(spark):

	_ =spark.udf.register("unaccent_py", unaccent_py, StringType())
	_ =spark.udf.register("scholar_semester", scholar_semester, StringType())

def unaccent_py(text):
    if text is not None:
        return unidecode(text)
    else:
        return None        

def scholar_semester(input_date):
    if input_date is not None:
        return str(input_date.year + list([0.1, 0.1, 0.1, 0.2, 0.2, 0.2, 0.2, 0.2, 0.2, 1.1, 1.1, 1.1])[input_date.month-1])
    else:
        return None
