"""
### CODE OWNERS: Bowen Li
### OBJECTIVE:
    Cleaning data with iterative rules in Pyspark
### DEVELOPER NOTES:
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql import functions as F
from pyspark.sql.functions import (
    regexp_replace,
    trim,
    col,
    when,
    lower,
    initcap,
    upper,
    regexp_extract,
)
from pathlib import Path
import logging
import os

logging.basicConfig(
    format="%(asctime)s - %(message)s", level=os.environ.get("LOGLEVEL", "INFO")
)
LOGGER = logging.getLogger(__name__)

# These are medical professional titles which will be used to remove
# This list should be constantly updated if unexpected new titles emerge
pattern_list = [
    "ACGNCGRA",
    "FNPCBCN",
    "FASCRS",
    "WHCNP",
    "MBBCH",
    "CCRNP",
    "ONNCG",
    "AOCNP",
    "MBBS",
    "CRNP",
    "CGRA",
    "MSPA",
    "ABCN",
    "FACS",
    "APRN",
    "WHNP",
    "LCGC",
    "PHD",
    "FACMG",
    "ACGN",
    "NNCG",
    "AOCN",
    "CBCN",
    "ONNCH",
    "MSNS",
    "ANRP",
    "AGCN",
    "CBCM",
    "FACP",
    "APNG",
    "CPNP",
    "WANP",
    "ACNP",
    "APNG",
    "APNP",
    "GCRA",
    "CRGA",
    "LGCG",
    "ARNP",
    "FNCN",
    "MFM",
    "SNH",
    "CCM",
    "MGC",
    "LGC",
    "OPP",
    "CGC",
    "APN",
    "CNM",
    "DNP",
    "CNP",
    "MSN",
    "FNP",
    "MMS",
    "RNP",
    "FNC",
    "LPN",
    "DPT",
    "PAC",
    "BSN",
    "MPH",
    "OCN",
    "MSM",
    "CGA",
    "NPC",
    "CGN",
    "GCC",
    "SGH",
    "ONC",
    "CNH",
    "MCN",
    "ANP",
    "CNS",
    "NPC",
    "FNO",
    "PCP",
    "MD",
    "MC",
    "PA",
    "PC",
    "RN",
    "ND",
    "DO",
    "GC",
    "PT",
    "PS",
    "PA",
    "MD",
    "MS",
    "NP",
    "RM",
    "CG",
    "CS",
]

# two lists need to be processed different because of speical chars
complex_pattern_list = [
    "ACNPT-CS",
    "AGPCNP-C",
    "P\.S\.NP-C",
    "ACNP-CP",
    "ACNP-CS",
    "ANCP-CS",
    "APNG-BC",
    "ARNP-BC",
    "CNH-SGH",
    "WHNP-BC",
    "ACN-CS",
    "ACP-CS",
    "AGN-BC",
    "AOCN-P",
    "FNP-BC",
    "FNP-CS",
    "NNP-CS",
    "ARN P",
    "FNP-C",
    "PA-CM",
    "RPA-C",
    "D\.O\.",
    "M\.D\.",
    "MS G",
    "P\.A\.",
    "P\.S\.",
    "PA_C",
    "PA-C",
    "-NP",
    "MD\.",
]

# ===========================================================


def cell_name_clean(readCSVFileDF):
    # Combine the elements from pattern_list to use in rid_credentials
    rid_credentials_list = ""
    for i in pattern_list:
        pattern = r"\b" + i + r"\b"
        rid_credentials_list += pattern + r"|"

    # Combine the elements from complex_pattern_list to use in rid_complex_credentials_list
    rid_complex_credentials_list = ""
    for i in complex_pattern_list:
        pattern = i
        # pattern = r"\b" + i + r"\b"
        rid_complex_credentials_list += pattern + r"|"

    # if dicts need to add more features, please be reminded that the order of the new features MATTER!
    patterns = {
        # char cleaning
        "kept_chars": r"[^A-Za-z,\.';\/\-\s&]",  # abc$% -> abc
        "slash_&semicolon_to_comma": r"\s*\/+\s*|;\s*",  # abc; cde -> abc cde
        "elim_extra_whitespaces": r"\s+",  #  _abc_ -> abc
        # style transformation
        "change_dr": r"\bDR\b\.*\s*|\bDR\b\s*|\bDR\b\s*\,\s*|\bDR\b,+\s*",  # DR.Lee -> DR. Lee
        "tf_and_to_comma": r"\s*\bAND\b\s*|\s*\&\s*",  # ab AND cd -> ab, cd
        # get rid of any credentials
        "rid_complex_credentials_list": rid_complex_credentials_list,  # abc MPH-CS -> abc
        "rid_credentials": rid_credentials_list,  # abc MD -> abc
        # style transformation again
        "dash_with_space": r"(\w+)\s*\-\s*(\w+)",  # abc-def -> abc def
        "Initial_with_a_dot": r"\.\s*[a-z]",  # D.rally -> D.Rally
        "dot_seperator_with_space": r"(\w)\.(\w+)",  #  abc.cde -> abc. cde
        "clean_ending_symbols": r"[,.\s]+$",  # abc... -> abc
        "comma_space_combo_allow_one": r",\s,\s|\s,\s",  # abc, , cde -> abc, cde
        "Rid of Dr.": r"^DR\.\s|^DR\.,|\sDR.\s|^\.dr\.\s",  # Dr. G Williams -> G Williams
    }

    replacements = {
        # clean weird characters
        "kept_chars": r"",
        "slash_&semicolon_to_comma": r", ",
        "elim_extra_whitespaces": r" ",
        # style transformation
        "change_dr": r"Dr. ",
        "tf_and_to_comma": r", ",
        # get rid of any credentials
        "rid_complex_credentials_list": "",
        "rid_credentials": "",
        # style transformation again
        "dash_with_space": r"$1 $2",
        "Initial_with_a_dot": r"\U$0",
        "dot_seperator_with_space": r"$1. $2",
        "clean_ending_symbols": "",
        "comma_space_combo_allow_one": ", ",
        "Rid of Dr.": r"",
    }

    # Start cleaning strings by eliminating any white spaces at the begining and at the end
    # Any Self-referring and null type will be converted to Self and None
    readCSVFileDF_tf = readCSVFileDF.withColumn(
        "Profession", trim(col("Profession"))
    ).withColumn(
        "Profession",
        when(
            lower(col("Profession")).isin(["self", "self-referred"]),
            "SELF",
        )
        .when(lower(col("Profession")).isin(["n/a", "null"]), None)
        .otherwise(col("Profession")),
    )

    # Run every pattern/replacement from the above dictionary to tranform name inputs
    # The order of every execution is very IMPORTANT!
    for pattern, replacement in dict(
        zip(patterns.values(), replacements.values())
    ).items():
        readCSVFileDF_tf = readCSVFileDF_tf.withColumn(
            "Profession",
            regexp_replace(upper(F.col("Profession")), pattern, replacement),
        )

    readCSVFileDF_tf = readCSVFileDF_tf.withColumn(
        "Profession", initcap(lower(col("Profession")))
    )

    return readCSVFileDF_tf


def main():
    # please put the script and test_data in the same folder
    path = Path.cwd()

    spark = SparkSession.builder.appName("Clean Text").getOrCreate()
    readCSVFileDF = spark.read.option("header", True).csv(
        str(path) + "/<<Test Data>>t.csv"
    )

    cleaned_df = cell_name_clean(readCSVFileDF)

    file_name = "clean_df"

    with open(path, "w") as file:
        file.write(f"{file_name}.csv")


if __name__ == "__main__":
    main()
