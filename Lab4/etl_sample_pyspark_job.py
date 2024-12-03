"""
Description : Sample script to read and write using amorphic utils - Pyspark

Date        : 12/Nov/2024
Author      : aditya.bhat@cloudwick.com
Version     : 1.0
Dependencies : amporphicutils
Glue Version : 3
Python Version : 3
Language : Python
ParamStore : SYSTEM.S3BUCKET.LZ , SYSTEM.S3BUCKET.LZ

Update History :
Date            Version			Author                                  Description
12/Nov/2024		1.0             aditya.bhat@cloudwick.com                   First version

"""

import sys
import time
import pandas as pd
import logging
from awsglue.utils import getResolvedOptions
from pyspark.sql.functions import lit, col, concat
from amorphicutils.common import read_param_store
from amorphicutils.pyspark.infra.gluespark import GlueSpark
from amorphicutils.pyspark import read
from amorphicutils.pyspark import write

logging.basicConfig(level="INFO")
LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)

glue_spark = GlueSpark()
glue_context = glue_spark.get_glue_context()
spark = glue_spark.get_spark()


def get_param_value(param_store_key, secure=False):
    """Function to read parameters from parameters store"""
    LOGGER.info(
        "In get_param_value, Fetching values stored  in %s, IsSecure : %s",
        param_store_key,
        secure,
    )
    param_value_response = read_param_store(param_store_key, secure=secure)
    if param_value_response["exitcode"] == 1:
        LOGGER.error(
            "Failed to get value for parameter {} with error {}.".format(
                param_store_key, param_value_response["message"]
            )
        )
        raise Exception(
            "Failed to get value for parameter {} with error {}.".format(
                param_store_key, param_value_response["message"]
            )
        )
    else:
        return param_value_response["data"]


def main():
    """
    Main
    """

    LOGGER.info("In Main,")
    args = getResolvedOptions(sys.argv, ['JOB_NAME', 'environment'])
    print(args['JOB_NAME'])
    print(args['environment'])

    user_id = "harsha"  # Your Login UserId with access to write

    # Replace below if you have different Domain to to Read from
    r_domain = "bronzehealth"
    # Replace this if you have different Domain to write into.
    w_domain = "silverhealth"
    # Replace this if you created new Dataset to Read from.
    r_dataset = "Immunization" 
    # Replace this if you created new Dataset to write into.
    w_dataset = "Immunization_Staged" 

    # LZ Bucket is Output Bucket
    # DLZ Bucket is Input Bucket
    # lz_bucket = "nvd-us-west-2-816069158041-test-lz"
    # dlz_bucket = "nvd-us-west-2-816069158041-test-dlz"

    lz_bucket = get_param_value(param_store_key="SYSTEM.S3BUCKET.LZ")
    dlz_bucket = get_param_value(param_store_key="SYSTEM.S3BUCKET.DLZ")
    my_param = get_param_value(
        param_store_key="nvd-sample-parm-adityabhat", secure=True
    )  # Replace with your paramstore name
    print(my_param)

    # Read dataset
    LOGGER.info("In Main, Reading Dataset")

    # Instantiate Reader and Writer Class
    amporphic_reader = read.Read(dlz_bucket, spark)
    amorphic_writer = write.Write(lz_bucket, glue_context)

    # Read dataset
    response = amporphic_reader.read_csv_data(r_domain, r_dataset, header=True)
    print(response)
    if response["exitcode"] == 0:
        LOGGER.info("Successfully Read Data")
        df_data = response["data"]
    else:
        raise Exception(
            "Error while reading data from {} .ERROR:{}".format(
                r_dataset, response["message"]
            )
        )

    df_data.head(5)
    print("Input Total Record Count", df_data.count())

    # Transformations if any....
    LOGGER.info("In Main, Successfully read Dataset")
    LOGGER.info("In Main, Applying Transformation")

    # Derive New Columns
    df_copy = df_data.withColumn(
        "client_full_name",
        concat(col("client_first_name"), lit(" "), col("client_last_name")),
    ).withColumn(
        "Address",
        concat(
            lit("Apt "),
            col("apartment").cast("string"),
            lit(" St "),
            col("street_number").cast("string"),
            lit(" "),
            col("primary_direction"),
            lit(" "),
            col("street_name"),
            lit(" "),
            col("street_type"),
            lit(" "),
            col("secondary_direction"),
            lit(" Post "),
            col("po_box").cast("string"),
            lit(" "),
            col("city"),
            col("state"),
            lit(" ZIP: "),
            col("zip_code").cast("string"),
        ),
    )

    # Drop unwanted columns
    drop_col_list = [
        "client_first_name",
        "client_last_name",
        "client_suffix",
        "client_age",
        "client_alias_last_name",
        "client_alias_first_name",
        "client_alias_middle_name",
        "client_marital_status",
        "primary_race",
        "race_2",
        "race_3",
        "race_4",
        "race_5",
        "ethnicity",
        "client_ssn",
        "primary_care_first_name",
        "primary_care_middle_name",
        "primary_care_last_name",
        "primary_care_suffix",
        "patient_program_close_date",
        "patient_program_close_reason",
        "mother_maiden_name",
        "patient_notes",
        "street_number",
        "primary_direction",
        "street_name",
        "street_type",
        "secondary_direction",
        "po_box",
        "apartment",
        "city",
        "state",
        "zip_code",
        "primary_contact_last_name",
        "primary_contact_first_name",
        "primary_contact_relationship_type",
        "primary_contact_home_phone",
        "primary_contact_cell_phone",
        "primary_contact_work_phone",
        "patient_precautions_contraindications_type",
        "precautions_contraindications_effective_date",
        "lot_number",
        "administration_site",
        "observation_period",
        "reaction",
        "status",
        "administered_indicator",
        "administered_by",
        "administered_at",
        "dose_sequence",
    ]
    df_copy = df_copy.drop(*drop_col_list)

    LOGGER.info("In Main, Transformation Complete")

    LOGGER.info("In Main, Writing to Dataset....")

    # Write to Dataset
    response = amorphic_writer.write_csv_data(
        df_copy.coalesce(1), w_domain, w_dataset, user=user_id, full_reload=False
    )
    # print(response)
    if response["exitcode"] == 0:
        LOGGER.info("In Main, Successfully wrote data to %s dataset ....", w_dataset)
    else:
        raise Exception(
            "Error writing data to %s dataset .ERROR: %s",
            w_dataset,
            response["message"],
        )

    print("End of Script")


if __name__ == "__main__":
    main()
