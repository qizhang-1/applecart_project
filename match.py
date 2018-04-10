from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


class DataProcessing(object):

    def __init__(self, resume_data_file_path, political_data_file_path, matched_data_filename, ambiguous_data_filename):

        """
        :param resume_data_file_path:  string,  input resume data path
        :param political_data_file_path: string, input political data path
        """

        self.resume_data_file_path = resume_data_file_path
        self.political_data_file_path = political_data_file_path
        self.political_df, self.resume_df, self.joined_df = None, None, None
        self.multiple_match_df, self.single_match_df = None, None
        self.matched_data_filename = matched_data_filename
        self.ambiguous_data_filename = ambiguous_data_filename
        self.spark = None

    def spark_session_initialization(self, project_name, warning_level="WARN"):
        """

        :param project_name: string,
        :param warning_level: string, "ERROR", "WARN", default is "WARN"
        :return:
        """
        if warning_level not in set(["ALL", "DEBUG", "ERROR", "FATAL", "TRACE", "WARN", "INFO", "OFF"]):
            warning_level = "ALL"
            print("warning level NOT FOUND! automatically set to be ALL!")

        self.spark = SparkSession.builder \
            .master('local[*]') \
            .appName(project_name) \
            .getOrCreate()
        self.spark.sparkContext.setLogLevel(warning_level)

    def spark_session_termination(self):
        self.spark.stop()

    def spark_read_files(self):

        """

        :return:
        """
        self.political_df = self.spark.read.csv(path=self.resume_data_file_path, header=True)
        self.resume_df = self.spark.read.csv(path=self.political_data_file_path, header=True)

    def political_data_cleaning(self):

        """
        clean the resume data
        1. convert the data to upper case
        2. remove the names in the bracket

        :return:
        """
        # data cleaning
        upper, clean_name = self._upper, self._clean_name
        schema = self.political_df.schema
        rdd1 = self.political_df.rdd.map(tuple)
        # upper casing names and city
        rdd2 = rdd1.map(lambda x: (x[0], upper(x[1]), upper(x[2]), upper(x[3]), x[4], x[5]))
        rdd3 = rdd2.map(lambda x: (x[0], clean_name(x[1]), clean_name(x[2]), x[3], x[4], x[5]))
        self.political_df = self.spark.createDataFrame(rdd3, schema)
        # cast birth_year to int and drop the gender column
        self.political_df = self.political_df.withColumn('birth_year', self.political_df['birth_year'].cast(IntegerType()))
        self.political_df = self.political_df.drop('gender')

    def resume_data_cleaning(self):
        """
        clean the resume data
        1. convert the data to upper case
        2. remove the names in the bracket
        3. extract the city name from the city, state string
        :return:
        """
        schema = self.resume_df.schema
        upper, clean_name, degree_match, extract_city, estimate_birth_year = \
            self._upper, self._clean_name, self._degree_match, self._extract_city, self._estimate_birth_year
        rdd1 = self.resume_df.rdd.map(tuple)
        # convert all the cases to upper cases names and city
        rdd2 = rdd1.map(lambda x: (x[0], upper(x[1]), upper(x[2]), upper(x[3]),
                                   x[4], upper(x[5])))
        # extract city from local_region
        rdd3 = rdd2.map(lambda x: (x[0], clean_name(x[1]), clean_name(x[2]), degree_match(x[3]),
                                   x[4], extract_city(x[5])))

        rdd4 = rdd3.map(lambda x: (x[0], x[1], x[2], estimate_birth_year(x[3], x[4]), x[4], x[5]))
        # modify schema
        self.resume_df = self.spark.createDataFrame(rdd4, schema)

        # cast birth_year to int and drop the gender column
        self.resume_df = self.resume_df.withColumn('estimated_birth_year', self.resume_df['degree'].cast(IntegerType()))
        self.resume_df = self.resume_df.drop('degree_start').drop('degree')

    def show_dataframe_head(self, nlines=10):
        """

        :param nlines: number of lines
        :return:
        """
        self.political_df.show(nlines, False)
        self.resume_df.show(nlines, False)

    def join_and_match(self):
        print("==========================================================")
        print("total number of resume ids: = " + str(self.resume_df.count()))
        print("==========================================================")

        self.joined_df = self.resume_df.join(self.political_df,
                        (self.resume_df['first_name'] == self.political_df['first_name'])
                      & (self.resume_df['last_name'] == self.political_df['last_name'])
                      & (self.resume_df['local_region'] == self.political_df['city'])
                      & (self.resume_df['estimated_birth_year'] >= self.political_df['birth_year'])
                      , 'left_outer')
        self.joined_df = self.joined_df.drop('city')
        self.joined_df = self.joined_df.filter(self.joined_df["political_id"].isNotNull())
        self.joined_df = self.joined_df.groupBy("resume_id").count()
        self.single_match_df = self.joined_df.filter(self.joined_df["count"] == 1)
        self.multiple_match_df = self.joined_df.filter(self.joined_df["count"] > 1)

        print("total number of exact matched resume ids: = " + str(self.single_match_df.count()))
        print("==========================================================")
        print("total number of multiple matched resume ids: = " + str(self.multiple_match_df.count()))
        print("==========================================================")

    def save_result(self):
        self.single_match_df.select('resume_id', 'count').toPandas().to_csv(self.matched_data_filename)
        self.multiple_match_df.select('resume_id', 'count').orderBy(['count'], ascending=[1]).toPandas().to_csv(self.ambiguous_data_filename)

    @staticmethod
    def _clean_name(input_string):

        """
        remove the names in the bracket, people may use alias in the resume
        :param input_string: first name
        :return: extracted first name
        """
        if not input_string:
            return None
        idx1 = input_string.find("(")
        idx2 = input_string.find(" ")
        idx3 = input_string.find(",")
        if idx1 == -1 and idx2 == -1 and idx3 == -1:
            return input_string
        idx1 = len(input_string) if idx1 == - 1 else idx1
        idx2 = len(input_string) if idx2 == - 1 else idx2
        idx3 = len(input_string) if idx3 == - 1 else idx3
        return input_string[:min(idx1, idx2, idx3)].strip()

    @staticmethod
    def _extract_city(input_string):

        """
        extract the city name from the string
        :param input_string: city, state
        :return: city name only
        """

        if not input_string:
            return None
        return input_string.split(",")[0].strip()

    @staticmethod
    def _upper(input_string):
        """
        convert the string to upper cases
        :param input_string: input string
        :return: string with upper cases only
        """
        return None if input_string is None else str.upper(input_string)

    @staticmethod
    def _degree_match(input_string):
        """
        The purpose of the function is to estimate the age of a person start the degree.
        Generally, BS is estimated at 18, MS is estimated at 22 and PhD is estimated at 24
        Any list not in BS, MS or PhD is set to be 20, an estimated default value

        :param input_string: degree string
        :return: expected degree start year (estimate, BS 18, MS 22, PHD 24 and others 20)
        """
        def contains_string(token_set, degree_string):
            return any(token in degree_string for token in token_set)

        bachelor = set(['B.S', 'BACHELOR', 'BS', 'B.A', 'BASC'])
        master = set(['M.S', 'MASTER', 'M.A'])
        phd = set(['PHD', 'PH.D', 'DOCTRATE', 'DOCTOR', 'DR'])
        if not input_string:
            return 18
        elif contains_string(bachelor, input_string):
            return 16
        elif contains_string(master, input_string):
            return 19
        elif contains_string(phd, input_string):
            return 22
        else:
            return 18

    @staticmethod
    def _estimate_birth_year(degree_age, degree_year):
        """

        :param degree_age: int, age starts the degree
        :param degree_year: degree start year
        :return: int, the estimated birth year
        """
        if not degree_year:
            return 1920
        return int(degree_year) - int(degree_age)

if __name__ == '__main__':

    # start spark session

    political_file_path = "./political_data_vendor.csv"
    resume_file_path = "./resume_data_vendor.csv"
    matched_filename = "./matched_data.csv"
    ambiguous_filename = "./ambiguous_data.csv"

    data_processing = DataProcessing(political_file_path, resume_file_path, matched_filename, ambiguous_filename)
    data_processing.spark_session_initialization('match_project', 'WARN')
    data_processing.spark_read_files()

    data_processing.political_data_cleaning()
    data_processing.resume_data_cleaning()
    data_processing.show_dataframe_head(20)

    data_processing.join_and_match()
    data_processing.save_result()
    data_processing.spark_session_termination()
