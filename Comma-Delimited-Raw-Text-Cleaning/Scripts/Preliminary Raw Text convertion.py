"""
### CODE OWNERS: Bowen Li
### OBJECTIVE: Cleaning rows distorted by commas within text in a csv file 
### DEVELOPER NOTES:
    - When a user exports data from a system and stores it in a CSV format due to it being comma-delimited, 
    - there's a likelihood that text containing commas might unintentionally be treated as delimiters. 
    - Consequently, this can cause the CSV to misinterpret column values and incorrectly separate the lines.
"""

import re
import os


def append_rows_with_missing_comma_and_short_row_with_comma(csv_content):
    # Splite the rows by space
    rows = csv_content.split("\n")

    # Append the rows with certain rules
    # In this case, if a row started with anything but a comma
    # then followed up by a comma, and repeated for 1-3 times and some digits afterward
    # Then this row is counted as a new row; otherwise it would be appeneded to the previous row
    appended_rows = []
    pattern = r"^([^,]*,){1,3}\d+,"

    current_row = ""

    for row in rows:
        if (
            current_row
            and not current_row.endswith(",")
            and not current_row.endswith("Other:")
        ):
            current_row += " " + row
        elif (
            current_row
            and not re.search(pattern, row[:80])
            and not current_row.endswith("Other:")
            and current_row.endswith(",")
        ):
            current_row += row
        else:
            if current_row:
                appended_rows.append(current_row)

            # Update the current_row to the current row for the next iteration
            current_row = row

    # Add the last current_row
    if current_row:
        appended_rows.append(current_row)

    # Join the processed lines back together with '\n' separator
    result = "\n".join(appended_rows)

    return result


def process_lines(raw_data):
    lines = raw_data.split("\n")

    processed_lines = []

    for line in lines:
        # Recognizing the Column line
        if line.startswith("keyword"):
            processed_lines.append(line)
        else:
            # Find the position of the first digit + a comma
            # It is assuming that any values sepearted by comma and before an all digits column
            # should be aggregated into one cell
            first_digit_position = -1
            for i, char in enumerate(line):
                if char.isdigit() and line[i - 1] == ",":
                    first_digit_position = i - 1
                    break

            if first_digit_position > 0:
                # Remove all commas except the last one before the digits
                processed_line = (
                    line[:first_digit_position].replace(",", "")
                    + line[first_digit_position:]
                )
            else:
                # If no such pattern is found, keep the line as is
                processed_line = line

            processed_lines.append(processed_line)

    result = "\n".join(processed_lines)

    return result


def main():
    # read IndividualDataDump as a raw file
    csv_file_path = (
        os.path.dirname(os.path.abspath(__file__)) + "/<<Your Raw File>>.csv"
    )

    with open(csv_file_path, "r", encoding="utf-8", errors="ignore") as file:
        raw_content = file.read()

    raw_content_rid_quotation_marks = raw_content.replace('"', "'")

    rawfile_with_each_row_a_patient = (
        append_rows_with_missing_comma_and_short_row_with_comma(
            raw_content_rid_quotation_marks
        )
    )

    combine_pedigree_names = process_lines(rawfile_with_each_row_a_patient)

    output_file_path = "Raw_File_Cleaned.csv"

    with open(output_file_path, "w") as file:
        file.write(combine_pedigree_names)


if __name__ == "__main__":
    main()
