"""
Data Wrangling Assignment – 3 (150 Points)
In this assignment we will use basic ideas of Set Theory to wrangle a clinical dataset
using a programming language of your choice. Submit your code and datasets using your
GitHub repository. Here is a quick refresher to Set Theory: htps://
www.geeksforgeeks.org/set-theory/. These concepts are used to deﬁne inclusion and
exclusion criteria of cohorts and in creation of their corresponding datasets (htps://
pubmed.ncbi.nlm.nih.gov/24026307/). COVID-19 has been associated with the
occurrence new diabetes and hyperglycemia (htps://academic.oup.com/jamiaopen/
article/4/3/ooab063/6320067). In this exercise you will use a synthetic diagnosis ﬁle
containing patient IDs, ICD 10 diagnosis codes, and a date of diagnosis. You will need to
use the following code sets for your wrangling steps:
• Diabetes Codes
ICD 10 Code Concept
E08 Diabetes mellitus due to underlying condition
E09 Drug or chemical induced diabetes mellitus
E10 Type 1 diabetes mellitus
E11 Type 2 diabetes mellitus
E13 Other speciﬁed diabetes mellitus
• COVID Codes
ICD 10 Code Concept
U07.1 COVID-19
J12.82 Pneumonia due to COVID-19
Questions:
1. Diabetes Set: (20 Points)
a. Find all patients with Diabetes using the codes above by listing their patient IDs.
b. Find the cardinality of the Diabetes set.
"""

from matplotlib import pyplot as plt
from matplotlib_venn import venn3
from pyspark.sql.functions import col, min

from resources import config
from src.main.utility.logging_config import logger
from src.main.utility.spark_session import spark_session

import matplotlib
matplotlib.use('TkAgg')


logger.info("************** Starting the Spark Session ************")
spark = spark_session()
logger.info("******************* Spark Session Created *******************")

logger.info("******************* Creating the Data frame from the synthetic Diagnosis file *********************")
# Read the CSV file
synthetic_diagnosis_file_df = spark.read.csv(config.DW3_set_exercise_file_path, header=True, inferSchema=True)

# Convert the "Patient ID" column from float to integer
synthetic_diagnosis_file_df = synthetic_diagnosis_file_df.withColumn("Patient ID", col("Patient ID").cast("integer"))

synthetic_diagnosis_file_df = synthetic_diagnosis_file_df.select(col("Patient ID"), col("Diagnosis Code"), "Date")
# synthetic_diagnosis_file_df.show()
logger.info("******************* Dataframe from the synthetic Diagnosis file is created *********************")

logger.info("**************** Filtering the records based on Diabetes codes ******************")
# Filter for Diabetes codes
diabetes_codes = ["E08", "E09", "E10", "E11", "E13"]
diabetes_df = synthetic_diagnosis_file_df.filter(synthetic_diagnosis_file_df["Diagnosis Code"].isin(diabetes_codes))

logger.info("******************** List patient IDs with Diabetes **********************")
# List patient IDs with Diabetes
diabetes_patient_ids = diabetes_df.select("Patient ID").distinct()
diabetes_patient_ids.show(diabetes_patient_ids.count())

logger.info("******************** Calculating the cardinality of Diabetes dataframe **********************")
# Find cardinality of the Diabetes set
cardinality = diabetes_df.distinct().count()
print(f"Cardinality of the Diabetes set: {cardinality}")

"""
2. COVID Set: (20 Points)
a. Find all patients with COVID using the codes above by listing their patient IDs.
b. Find the cardinality of the COVID set.
"""

logger.info("**************** Filtering the dataframe using COVID Codes ****************")
# Filter for COVID codes
covid_codes = ["U07.1", "J12.82"]
covid_df = synthetic_diagnosis_file_df.filter(synthetic_diagnosis_file_df["Diagnosis Code"].isin(covid_codes))

logger.info("***************** Listing patient IDs with covid *******************")
# List patient IDs with covid
covid_patient_ids = covid_df.select("Patient ID").distinct()
covid_patient_ids.show(covid_patient_ids.count())

logger.info("***************** Finding cardinality of the Diabetes set ******************")
# Find cardinality of the Diabetes set
cardinality = covid_df.distinct().count()
print(f"Cardinality of the COVID set: {cardinality}")

"""
3. Intersection Set (20 Points)
a. Find all patients with Diabetes and COVID using the codes above by listing their patient IDs.
b. Find the cardinality of the Intersection set.
"""

logger.info("********************** Filtering for Diabetes and COVID codes separately *********************")
# Filter for Diabetes and COVID codes separately
diabetes_df = synthetic_diagnosis_file_df.filter(col("Diagnosis Code").isin(diabetes_codes))
covid_df = synthetic_diagnosis_file_df.filter(col("Diagnosis Code").isin(covid_codes))

logger.info(
    "*********************** Finding the intersection of patient IDs with Diabetes and COVID *******************")
# Find the intersection of patient IDs with Diabetes and COVID
intersection_patient_ids = diabetes_df.join(covid_df, on="Patient ID", how="inner")

logger.info("********* Select distinct patient IDs to get the intersection **********")
# Select distinct patient IDs to get the intersection
intersection_patient_ids = intersection_patient_ids.select("Patient ID").distinct()

logger.info("************* Show the patient ids ***********")
# Show the patient IDs
intersection_patient_ids.show(intersection_patient_ids.count())

logger.info("************ Find the cardinality of the Intersection set ****************")
# Find the cardinality of the Intersection set
cardinality = intersection_patient_ids.count()
logger.info(f"Cardinality of the Intersection set: {cardinality}")

"""
4. Union Set (20 Points)
a. Find all patients with Diabetes or COVID using the codes above by listing their patient IDs.
b. Find the cardinality of the Intersection set.
"""
logger.info("************ Filtering for Diabetes or COVID codes ************")
# Filter for Diabetes or COVID codes
union_df = synthetic_diagnosis_file_df.filter(
    synthetic_diagnosis_file_df["Diagnosis Code"].isin(diabetes_codes + covid_codes))

logger.info("********** List patient IDs with Diabetes or COVID ***********")
# List patient IDs with Diabetes or COVID
union_patient_ids = union_df.select("Patient ID").distinct()
union_patient_ids.show(union_patient_ids.count())

# Find the cardinality of the Intersection set
cardinality = union_patient_ids.count()
logger.info(f"Cardinality of the Intersection set: {cardinality}")

# 5. Draw a Venn diagram showing the Diabetes, COVID, Intersection and Union sets. You might need to use a package. (
# 40 points)


# counts:
total_patients = synthetic_diagnosis_file_df.distinct().count()
diabetes_patients = diabetes_patient_ids.count()
covid_patients = covid_patient_ids.count()
intersection_patients = intersection_patient_ids.count()
union_patients = union_patient_ids.count()

# Calculate the sizes of the sets
diabetes_only = diabetes_patients - intersection_patients
covid_only = covid_patients - intersection_patients

logger.info("************* Drawing the Venn diagram, KIndly close the interactive window "
            "so that the code can execute further ***************")
# Draw the Venn diagram
venn3([set(range(diabetes_only)), set(range(covid_only)), set(range(intersection_patients))],
      ('Diabetes', 'COVID', 'Intersection'))

logger.info("************** Plotting the venn diagram **************")
plt.show()

"""
6. Diabetes only a�er COVID Set (30 points)
a. Now including the date of diagnosis, ﬁnd all patients with Diabetes only after they had COVID by listing their patient IDs.
b. Find the cardinality of the Diabetes only after COVID set.
c. Provide a count breakdown for each of the diabetes codes listed above occurring only after COVID.
"""

logger.info("******* Finding the minimum date for COVID diagnosis for each patient *********")
# Find the minimum date for COVID diagnosis for each patient
covid_min_dates = covid_df.groupBy("Patient ID").agg(min("Date").alias("Min_COVID_Date"))

logger.info("************* Joining Diabetes patients with the minimum COVID date ***************")
# Join Diabetes patients with the minimum COVID date
diabetes_after_covid_df = diabetes_df.join(covid_min_dates, on="Patient ID", how="inner")

logger.info("************** Filtering for Diabetes patients diagnosed after COVID **************")
# Filter for Diabetes patients diagnosed after COVID
diabetes_after_covid_df = diabetes_after_covid_df.filter(col("Date") > col("Min_COVID_Date"))
diabetes_after_covid_df.select("Patient ID").show(diabetes_after_covid_df.count())

logger.info("*********** Finding the cardinality of the Diabetes only after COVID set *************")
# Find the cardinality of the Diabetes only after COVID set
cardinality = diabetes_after_covid_df.select("Patient ID").distinct().count()
logger.info(f"Cardinality of the Diabetes only after COVID set: {cardinality}")

logger.info("********** Providing a count breakdown for each Diabetes code occurring only after COVID *************")
# Provide a count breakdown for each Diabetes code occurring only after COVID
diabetes_after_covid_counts = diabetes_after_covid_df.groupBy("Diagnosis Code").count()
diabetes_after_covid_counts.show()

logger.info("************* Stopping the Spark session ***************")
# Stop the SparkSession
spark.stop()
logger.info("************* spark session is stopped ***************")
