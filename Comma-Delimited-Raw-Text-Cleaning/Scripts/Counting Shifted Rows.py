"""
### CODE OWNERS: Bowen Li
### OBJECTIVE: Distinguishing rows with incorrect column positions 
### DEVELOPER NOTES:
    - Taking a position of a column, and take judgement call on what type/pattern you expect of this column
    - Then check if the value in each row are positioned in the correct index
"""

import os
import pandas as pd


def correct_comma_number(raw_data, col_name):
    # This function assumes the columns are the correctly separated by comma
    # And it counts the # of columns as default it as the correct colums of the dataframe

    lines = raw_data.split("\n")
    column_row = lines[0]
    column_value_to_find = col_name
    index_of_keyword = column_row.find(column_value_to_find)
    number_of_comma_before_keyword = column_row[:index_of_keyword].count(",")

    return number_of_comma_before_keyword


def extract_counts_and_values_between_commas(
    raw_data, correct_comma_count, key_pattern
):
    # Split the raw data into lines
    lines = raw_data.split("\n")

    # Initialize empty lists to store the comma counts

    Line_need_clean = []
    Line_untouched = []

    column_row = lines[0]
    # creating the new column as the first column in raw text format
    new_column_row = "num_count," + column_row
    Line_need_clean.append(new_column_row)
    Line_untouched.append(new_column_row)

    for line in lines[1:]:
        # Find the position of the first keyword
        key_position = line.find(key_pattern)

        if key_position != -1:
            # Count the commas before the first keyword
            comma_count = line[:key_position].count(",")
            if comma_count != correct_comma_count:
                Line_need_clean.append(
                    str(comma_count - correct_comma_count) + "," + line
                )
            else:
                Line_untouched.append(
                    str(comma_count - correct_comma_count) + "," + line
                )
        else:
            Line_need_clean.append("-1000," + line)

    Leave_alone_df = "\n".join(Line_untouched)
    Need_clean_df = "\n".join(Line_need_clean)

    return Leave_alone_df, Need_clean_df


def main():
    # read IndividualDataDump as a raw file
    csv_file_path_directory = os.path.dirname(os.path.abspath(__file__))
    csv_file_path_name = "/master_file_cleaned.csv"
    csv_file_path = csv_file_path_directory + csv_file_path_name

    with open(csv_file_path, "r", encoding="utf-8", errors="ignore") as file:
        raw_content = file.read()

    # Define the column to filter
    col_name = "keyword column"  # <- needs to define
    key_pattern = "keyword"  # <- needs to define
    correct_count = correct_comma_number(raw_content, col_name)

    leave_alone, needs_clean = extract_counts_and_values_between_commas(
        raw_content, correct_count, key_pattern
    )

    file_name = col_name
    untouch_output_file_path = f"clean_and_untouched/untouched_{file_name}.csv"
    clean_output_file_path = f"clean_and_untouched/needs_clean_{file_name}.csv"

    with open(untouch_output_file_path, "w") as file:
        file.write(leave_alone)

    with open(clean_output_file_path, "w") as file:
        file.write(needs_clean)


if __name__ == "__main__":
    main()
