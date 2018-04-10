from pyspark.sql import SparkSession
if __name__ == '__main__':

    spark = SparkSession.builder \
        .master('local[*]') \
        .appName('MatchingProject') \
        .getOrCreate()

    political_path = "./political_data_vendor.csv"
    political_df = spark.read.csv(path=political_path, header=True)
    political_df.printSchema()
    political_schema = political_df.schema
    print("number of political data: {}".format(political_df.count()))
    political_df.show(10, False)

    resume_path = "resume_data_vendor.csv"
    resume_df = spark.read.csv(path=resume_path, header=True)
    resume_df.printSchema()
    resume_schema = resume_df.schema
    print("number of resume data: {}".format(resume_df.count()))
    resume_df.show(10, False)


    def upper(s):
        return None if s is None else str.upper(s)

    # Data cleaning
    p_rdd1 = political_df.rdd.map(tuple)
    # upper casing names and city
    p_rdd2 = p_rdd1.map(lambda x: (x[0], upper(x[1]), upper(x[2]), upper(x[3]), x[4], x[5]))
    political_df = spark.createDataFrame(p_rdd2, political_schema)
    political_df.show(10, False)

    r_rdd1 = resume_df.rdd.map(tuple)
    # upper casing names and city
    r_rdd2 = r_rdd1.map(lambda x: (x[0], upper(x[1]), upper(x[2]), upper(x[3]), x[4], upper(x[5])))
    # extract city from local_region
    r_rdd3 = r_rdd2.map(lambda x: (x[0], x[1], x[2], x[3], x[4], x[5].split(",")[0]))
    resume_df = spark.createDataFrame(r_rdd3, resume_schema)
    resume_df.show(10, False)

    spark.stop()