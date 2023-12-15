"""
### CODE OWNERS: Bowen Li
### OBJECTIVE: combine two cleaned up files 
### DEVELOPER NOTES:
"""

import os
import re
import pandas as pd


def merge_2_frames(untouched, needs_clean):
    merged_lines = []

    needs_clean_lines = needs_clean.split("\n")
    untouched_lines = untouched.split("\n")

    for line in untouched_lines:
        merged_lines.append(line)

    for line in needs_clean_lines[1:]:
        merged_lines.append(line)

    merged = "\n".join(merged_lines)

    return merged


def main():
    # read IndividualDataDump as a raw file
    csv_file_path_directory = (
        os.path.dirname(os.path.abspath(__file__)) + "/clean_and_untouched"
    )

    csv_file_path_name_untouched = "/untouched_Privacy Policy.csv"

    csv_file_path_name_needs_clean = "/needs_clean_Privacy Policy.csv"

    csv_file_path_untouched = csv_file_path_directory + csv_file_path_name_untouched
    csv_file_path_needs_clean = csv_file_path_directory + csv_file_path_name_needs_clean

    with open(csv_file_path_untouched, "r", encoding="utf-8", errors="ignore") as file:
        raw_content_untouched = file.read()

    with open(
        csv_file_path_needs_clean, "r", encoding="utf-8", errors="ignore"
    ) as file:
        raw_content_needs_clean = file.read()

    merged = merge_2_frames(raw_content_untouched, raw_content_needs_clean)

    # Capture the name of a csv file
    pattern = r"_(.*?)\."

    match = re.search(pattern, csv_file_path_name_untouched)
    extracted_value = match.group(1)

    merged_output_file_path = f"merged_df_{extracted_value}.csv"

    with open(merged_output_file_path, "w") as file:
        file.write(merged)


if __name__ == "__main__":
    main()
