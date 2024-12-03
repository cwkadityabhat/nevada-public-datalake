"""
Description : Sample script to read and write using amorphic utils - Pythonshell

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
from amorphicutils.common import read_param_store
from amorphicutils.python import write
from amorphicutils.python import read

logging.basicConfig(level="INFO")
LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)


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

    user_id = "adityabhat"  # Your Login UserId with access to write
    
    # Replace below if you have different Domain to to Read from
    r_domain = "bronzehealth"
    # Replace this if you have different Domain to write into.
    w_domain = "silverhealth"
    # Replace this if you created new Dataset to Read from.
    r_dataset = "Immunization" 
    # Replace this if you created new Dataset to write into.
    w_dataset = "Immunization_Staged" 

    # LZ Bucket is Output Bucket
    # lz_bucket = "nvd-us-west-2-816069158041-test-lz"
    # DLZ Bucket is Input Bucket
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
    amporphic_reader = read.Read(dlz_bucket)
    amorphic_writer = write.Write(lz_bucket)

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
    df_copy = df_data.copy()
    df_copy["client_full_name"] = (
        df_data["client_first_name"] + " " + df_data["client_last_name"]
    )
    df_copy["Address"] = (
        "Apt "
        + df_data["apartment"].astype(str)
        + " St "
        + df_data["street_number"].astype(str)
        + " "
        + df_data["primary_direction"]
        + " "
        + df_data["street_name"]
        + " "
        + df_data["street_type"]
        + " "
        + df_data["secondary_direction"]
        + " Post "
        + df_data["po_box"].astype(str)
        + " "
        + df_data["city"]
        + df_data["state"]
        + " ZIP: "
        + df_data["zip_code"].astype(str)
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
    df_copy.drop(drop_col_list, axis=1, inplace=True)

    LOGGER.info("In Main, Transformation Complete")

    LOGGER.info("In Main, Writing to Dataset....")

    # Write to Dataset
    response = amorphic_writer.write_csv_data(
        df_copy, w_domain, w_dataset, user=user_id, full_reload=False
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
