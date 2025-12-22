import pandas as pd
import os
from pathlib import Path
from dateutil import parser
import re


def convert_sheet_name_to_date(sheet_name):
    """
    Convert sheet name to date. Handles various formats like:
    28-10-25, 28-Oct-25, 28.10.25, 28-October-25, etc.
    Format: day-month-year
    """
    try:
        # Clean the sheet name - remove extra spaces
        sheet_name = sheet_name.strip()

        # Replace dots with hyphens for consistency
        sheet_name = sheet_name.replace('.', '-')

        # Try to parse with dateutil parser (handles many formats automatically)
        # dayfirst=True ensures day-month-year order
        parsed_date = parser.parse(sheet_name, dayfirst=True)

        # Return as date string in standard format
        return parsed_date.strftime('%Y-%m-%d') 

    except Exception as e:
        # If parsing fails, return the original sheet name with a note
        return f"PARSE_ERROR: {sheet_name}"


def find_header_row(df):
    """
    Find the row containing 'Sl. No.' which marks the start of headers.
    Returns the row index and column index if found, (None, None) otherwise.
    """
    # Search through rows to find 'Sl. No.'
    for row_idx in range(min(20, len(df))):  # Search first 20 rows
        for col_idx in range(len(df.columns)):
            cell_value = str(df.iloc[row_idx, col_idx]).strip()
            if cell_value == 'Sl. No.':
                return row_idx, col_idx

    return None, None


def find_total_row(df, start_row, start_col, end_col):
    """
    Find the FIRST row containing 'Total' or 'Summary' (case-insensitive, with/without spaces or =).
    Searches across all columns from start_col to end_col.
    Returns the row index if found, None otherwise.
    """
    for idx in range(start_row + 1, len(df)):
        # Check all columns in the data range
        for col_idx in range(start_col, end_col + 1):
            cell_value = str(df.iloc[idx, col_idx]).strip().lower()
            # Remove spaces and equals signs for comparison
            cell_value_cleaned = cell_value.replace(' ', '').replace('=', '')

            if 'total' in cell_value_cleaned or 'summary' in cell_value_cleaned:
                return idx

    return None


def find_last_data_row(df, start_row, start_col, end_col):
    """
    Find the last row with actual data before blank rows.
    Used as fallback when 'Total' or 'Summary' is not found.
    Returns the row index of the last non-blank row, or None.
    """
    last_data_row = None

    for idx in range(start_row + 1, len(df)):
        # Check if any column in the range has data
        row_has_data = False
        for col_idx in range(start_col, end_col + 1):
            cell_value = str(df.iloc[idx, col_idx]).strip()
            if cell_value and cell_value.lower() != 'nan':
                row_has_data = True
                break

        if row_has_data:
            last_data_row = idx
        else:
            # If we found blank row after having data, stop searching
            if last_data_row is not None:
                break

    return last_data_row


def process_sheet(file_path, sheet_name):
    """
    Process a single sheet and extract data between header and Total row.
    Returns a dataframe or None if sheet doesn't meet criteria.
    """
    try:
        # Read the sheet without treating any row as header initially
        df = pd.read_excel(file_path, sheet_name=sheet_name, header=None)

        # Find header row starting with 'Sl. No.'
        header_row, start_col = find_header_row(df)
        if header_row is None:
            print(f"    ⚠ Skipped: 'Sl. No.' not found")
            return None

        # Find 'Sample' column to determine end column
        end_col = None
        for col_idx in range(start_col, len(df.columns)):
            cell_value = str(df.iloc[header_row, col_idx]).strip()
            if cell_value == 'Sample':
                end_col = col_idx
                break

        if end_col is None:
            print(f"    ⚠ Skipped: 'Sample' column not found")
            return None

        # Find Total or Summary row (first occurrence across all columns)
        total_row = find_total_row(df, header_row, start_col, end_col)

        if total_row is None:
            # If Total/Summary not found, find the last row with data
            last_data_row = find_last_data_row(
                df, header_row, start_col, end_col)
            if last_data_row is None:
                print(
                    f"    ⚠ Skipped: Could not determine end of data (no 'Total'/'Summary' and no data rows)")
                return None
            data_end = last_data_row + 1  # Include the last data row
            end_marker = "last data row"
        else:
            data_end = total_row  # Exclude the Total/Summary row
            end_marker = f"Total/Summary at row {total_row + 1}"

        # Extract data between header and end marker
        data_start = header_row + 1
        data_end = total_row

        if data_start >= data_end:
            print(f"    ⚠ Skipped: No data rows found between header and Total")
            return None

        # Extract only columns from 'Sl. No.' to 'Sample' (inclusive)
        headers = df.iloc[header_row, start_col:end_col +
                          1].astype(str).str.strip().tolist()

        # Extract the data rows for the specified columns only
        data_df = df.iloc[data_start:data_end, start_col:end_col+1].copy()
        data_df.columns = headers

        # Remove rows where all columns are blank/empty
        initial_row_count = len(data_df)
        # Drop rows where all values are NaN
        data_df = data_df.dropna(how='all')
        data_df = data_df[~data_df.astype(str).apply(lambda x: x.str.strip().eq(
            '').all(), axis=1)]  # Drop rows where all values are empty strings

        rows_removed = initial_row_count - len(data_df)

        if len(data_df) == 0:
            print(f"    ⚠ Skipped: All data rows were blank")
            return None

        # Add metadata columns
        data_df['Source_File'] = file_path.name
        data_df['Sheet_Name'] = sheet_name

        # Convert sheet name to date
        data_df['Date'] = convert_sheet_name_to_date(sheet_name)

        blank_info = f", {rows_removed} blank row(s) removed" if rows_removed > 0 else ""
        print(
            f"    ✓ Extracted {len(data_df)} rows (Header at row {header_row + 1}, {end_marker}{blank_info})")
        return data_df

    except Exception as e:
        print(f"    ✗ Error: {str(e)}")
        return None


def combine_excel_files(folder_path, output_file='combined_output.xlsx'):
    """
    Combine multiple .xls and .xlsx files (all sheets) into a single Excel file.
    
    Parameters:
    folder_path (str): Path to the folder containing Excel files
    output_file (str): Name of the output combined file (default: 'combined_output.xlsx')
    """

    all_dataframes = []

    # Get all .xls and .xlsx files from the folder
    excel_files = []
    for ext in ['*.xls', '*.xlsx']:
        excel_files.extend(Path(folder_path).glob(ext))

    if not excel_files:
        print(f"No Excel files found in {folder_path}")
        return

    print(f"Found {len(excel_files)} Excel file(s)\n")

    # Process each file
    for file_path in excel_files:
        print(f"Processing: {file_path.name}")

        try:
            # Get all sheet names
            xl_file = pd.ExcelFile(file_path)
            sheet_names = xl_file.sheet_names
            print(f"  Found {len(sheet_names)} sheet(s)")

            # Process each sheet
            for sheet_name in sheet_names:
                print(f"  Sheet: '{sheet_name}'")
                sheet_df = process_sheet(file_path, sheet_name)

                if sheet_df is not None:
                    all_dataframes.append(sheet_df)

        except Exception as e:
            print(f"  ✗ Error reading file: {str(e)}")
            continue

        print()  # Blank line between files

    if not all_dataframes:
        print("No valid data found to combine")
        return

    # Combine all dataframes
    print("Combining all valid sheets...")
    combined_df = pd.concat(all_dataframes, ignore_index=True)

    # Reorder columns to put metadata at the end
    cols = combined_df.columns.tolist()
    metadata_cols = ['Source_File', 'Sheet_Name', 'Date']
    data_cols = [col for col in cols if col not in metadata_cols]
    combined_df = combined_df[data_cols + metadata_cols]

    # Save to new Excel file
    output_path = os.path.join(folder_path, output_file)
    combined_df.to_excel(output_path, index=False)

    print(f"\n{'='*60}")
    print(f"✓ Successfully combined {len(all_dataframes)} sheet(s)")
    print(f"✓ Total rows: {len(combined_df)}")
    print(f"✓ Output saved to: {output_path}")
    print(f"{'='*60}")


if __name__ == "__main__":
    print("Excel Files Combiner - All Sheets with Dynamic Header Detection")
    print("="*60)

    folder = input("\nEnter the folder path containing Excel files: ").strip()

    if os.path.exists(folder):
        output_name = input(
            "Enter output filename (press Enter for 'combined_output.xlsx'): ").strip()
        if not output_name:
            output_name = 'combined_output.xlsx'
        elif not output_name.endswith('.xlsx'):
            output_name += '.xlsx'

        print()
        combine_excel_files(folder, output_name)
    else:
        print(f"Error: Folder '{folder}' does not exist")
