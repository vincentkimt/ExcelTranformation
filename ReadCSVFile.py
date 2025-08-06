import pandas as pd
import os
import pytz
import re
from dateutil.parser import parse
from typing import Callable, Optional

def process_csv_in_chunks(
    input_file: str,
    output_file: str,
    transform_function: Callable[[pd.DataFrame], pd.DataFrame],
    chunk_size: int = 50000,
    write_header: bool = True
) -> None:
    """
    Process a large CSV file in chunks to avoid memory issues.
    
    Args:
        input_file: Path to input CSV file
        output_file: Path to output CSV file
        transform_function: Function to apply transformations to each chunk
        chunk_size: Number of rows to process at once
        write_header: Whether to write header to output file
    """
    
    # Remove output file if it exists
    if os.path.exists(output_file):
        os.remove(output_file)
    
    # Check if deduplication is needed and get columns
    dedup_columns = get_deduplication_columns()
    print(dedup_columns)
    if dedup_columns:
        print(f"Global deduplication will be performed on columns: {dedup_columns}")
        # Use deduplication-aware processing
        _process_with_deduplication(input_file, output_file, transform_function, chunk_size, write_header, dedup_columns)
    else:
        # Use original processing logic
        _process_without_deduplication(input_file, output_file, transform_function, chunk_size, write_header)

def _process_without_deduplication(
    input_file: str,
    output_file: str,
    transform_function: Callable[[pd.DataFrame], pd.DataFrame],
    chunk_size: int,
    write_header: bool
) -> None:
    """processing logic without deduplication"""
    first_chunk = True
    total_rows_processed = 0
    
    try:
        # Read CSV in chunks
        for chunk_df in pd.read_csv(input_file, chunksize=chunk_size):
            # Apply transformations
            transformed_chunk = transform_function(chunk_df)
            
            # Write to output file
            mode = 'w' if first_chunk else 'a'
            header = write_header and first_chunk
            
            transformed_chunk.to_csv(
                output_file, 
                mode=mode, 
                header=header, 
                index=False
            )
            
            total_rows_processed += len(chunk_df)
            first_chunk = False
            
            # Optional: Print progress
            if total_rows_processed % 50000 == 0:
                print(f"Processed {total_rows_processed} rows...")
    
    except Exception as e:
        print(f"Error processing file: {e}")
        raise
    
    print(f"Successfully processed {total_rows_processed} rows")

def _process_with_deduplication(
    input_file: str,
    output_file: str,
    transform_function: Callable[[pd.DataFrame], pd.DataFrame],
    chunk_size: int,
    write_header: bool,
    dedup_columns: list
) -> None:
    """Processing logic with global deduplication"""
    
    # Step 1: Apply transformations and create temporary transformed file
    temp_transformed_file = input_file + '.temp_transformed.csv'
    print("Step 1: Applying transformations...")
    
    first_chunk = True
    total_rows_processed = 0
    
    try:
        for chunk_df in pd.read_csv(input_file, chunksize=chunk_size):
            # Apply transformations (but skip deduplication in transform function)
            transformed_chunk = transform_function(chunk_df, skip_deduplication=True)
            
            # Write transformed data to temp file
            mode = 'w' if first_chunk else 'a'
            header = write_header and first_chunk
            
            transformed_chunk.to_csv(
                temp_transformed_file,
                mode=mode,
                header=header,
                index=False
            )
            
            total_rows_processed += len(chunk_df)
            first_chunk = False
            
            if total_rows_processed % 50000 == 0:
                print(f"Transformed {total_rows_processed} rows...")
    
    except Exception as e:
        print(f"Error during transformation: {e}")
        # Clean up temp file
        if os.path.exists(temp_transformed_file):
            os.remove(temp_transformed_file)
        raise
    
    print(f"Transformation complete. {total_rows_processed} rows processed.")
    
    # Step 2: Perform global deduplication
    print("Step 2: Performing global deduplication...")
    _remove_duplicates_across_chunks(temp_transformed_file, output_file, dedup_columns, chunk_size)
    
    # Step 3: Clean up temp file
    if os.path.exists(temp_transformed_file):
        os.remove(temp_transformed_file)
    
    print("Processing with deduplication complete!")

def _remove_duplicates_across_chunks(input_file: str, output_file: str, dedup_columns: list, chunk_size: int) -> None:
    """
    Remove duplicates based on specified columns across entire CSV file when processing in chunks.
    """
    
    # Step 1: First pass - collect all unique combinations
    seen_combinations = set()
    first_occurrence_indices = {}  # Maps combination to (chunk_num, row_in_chunk)
    
    print("First pass: Identifying unique combinations...")
    
    chunk_reader = pd.read_csv(input_file, chunksize=chunk_size)
    
    for chunk_num, chunk in enumerate(chunk_reader):
        if chunk_num % 10 == 0:
            print(f"Processing chunk {chunk_num + 1} for duplicate detection...")
        
        for idx, row in chunk.iterrows():
            # Create combination tuple from specified columns
            combination = tuple(row[col] for col in dedup_columns if col in chunk.columns) #this give the combination of the cols given for identifying the duplicates. for example, Name, Grade i.e., Frank Jones,11

            #check if this combination has been noted into the seen_combination, if present then skip adding, otherwise add this combination
            #ex. if Frank Jones,11 is already present in seen_combinations then it won't add again
            if combination not in seen_combinations:
                seen_combinations.add(combination)
                first_occurrence_indices[combination] = (chunk_num, idx % chunk_size)
    
    print(f"Found {len(seen_combinations)} unique combinations")
    
    # Step 2: Second pass - write only first occurrences to output file
    print("Second pass: Writing deduplicated data...")
    
    chunk_reader = pd.read_csv(input_file, chunksize=chunk_size)
    first_chunk = True
    total_rows_written = 0
    
    for chunk_num, chunk in enumerate(chunk_reader):
        # Create mask for rows to keep (first occurrences only)
        rows_to_keep = []
        
        for idx, row in chunk.iterrows():
            combination = tuple(row[col] for col in dedup_columns if col in chunk.columns)
            expected_chunk, expected_idx = first_occurrence_indices[combination]
            
            # Keep row if this is its first occurrence
            if chunk_num == expected_chunk and (idx % chunk_size) == expected_idx:
                rows_to_keep.append(True)
            else:
                rows_to_keep.append(False)
        
        # Filter chunk to keep only first occurrences
        filtered_chunk = chunk[rows_to_keep]
        
        if len(filtered_chunk) > 0:
            # Write to output file
            if first_chunk:
                filtered_chunk.to_csv(output_file, index=False)
                first_chunk = False
            else:
                filtered_chunk.to_csv(output_file, mode='a', header=False, index=False)
            
            total_rows_written += len(filtered_chunk)
            if chunk_num % 10 == 0:
                print(f"Chunk {chunk_num + 1}: Kept {len(filtered_chunk)} rows")
    
    print(f"Deduplication complete! Total rows written: {total_rows_written}")

def get_deduplication_columns() -> Optional[list]:
    """
    Check rules file for deduplication requirements and return columns to deduplicate on.
    Returns None if no deduplication is needed.
    """
    try:
        reader = ExcelReader('C:\\Vincent\\Asset Project Learning - python\\CSVs\\Rules\\Rules.csv')
        data = reader.get_content()
        
        if data.empty or data.isnull().all().all():
            return None
        
        # Check for RemoveDuplicates rule
        for row_index, row in data.iterrows():
            for col_index, col in enumerate(data.columns):
                value = row[col]
                if pd.isna(value):
                    continue
                    
                value = str(value).strip().replace(" ", "")
                value = re.sub(r'\s+', '', value)
                
                if "RemoveDuplicates" in value:
                    dupli_txt = value.lower()
                    dupli_txt1 = dupli_txt.replace("removeduplicates", "")
                    if dupli_txt1:
                        duplicate_columns = dupli_txt1.split("/")
                        return [col.strip() for col in duplicate_columns if col.strip()]
        
        return None
        
    except Exception as e:
        print(f"Error reading rules file: {e}")
        return None

def data_transformation(df: pd.DataFrame, skip_deduplication: bool = False) -> pd.DataFrame:
    """
    Transformation functionss
    """
    # Example transformations:
    
    # 1. Clean string columns (remove whitespace)
    string_cols = df.select_dtypes(include=['object']).columns
    for col in string_cols:
        df[col] = df[col].astype(str).str.strip()
    
    # 2. Add a new calculated column (example)
    if 'price' in df.columns and 'quantity' in df.columns:
        df['total_value'] = df['price'] * df['quantity']
   
    # 3. Convert data types if needed
    #numeric_cols = ['price', 'quantity', 'total_value']
    #for col in numeric_cols:
    #    if col in df.columns:
    #        df[col] =   pd.to_numeric(df[col], errors='coerce')

    # Are there custom Rules?    
    reader = ExcelReader('C:\\Vincent\\Asset Project Learning - python\\CSVs\\Rules\\Rules.csv')
    data = reader.get_content()
        
    if data.empty:
        print("The DataFrame is empty.")

    # Check if all values in the DataFrame are NaN (null)
    elif data.isnull().all().all():
        print("The DataFrame contains only null values.")

    else:
        #print("There are custom rules")
        for row_index, row in data.iterrows():
            for col_index, col in enumerate(data.columns):
                value = row[col]
                if pd.isna(value):
                    continue
                    
                value = str(value).strip()
                value = value.replace(" ", "")
                value = re.sub(r'\s+', '', value)

                if isinstance(value, str) and "Lowercase" in value:
                    df.columns = df.columns.str.lower().str.replace(' ', '_')

                if "DefaultDateFormat" in value:
                    date_columns = ['order_date', 'created_date', 'delivery_date','dateofexam']  # Replace with your column names                   
                    
                    df.columns = df.columns.str.strip()
                    print(df.columns.tolist())
                    dateFormat = value.replace("DefaultDateFormat","")
                    if 'DateOfExam' in df.columns:
                        df['DateOfExam'] = df['DateOfExam'].apply(lambda x: format_if_date(x, dateFormat))

                if isinstance(value, str) and "RemoveEmptyRows" in value:
                    df = df.dropna()
                
                if "Average" in value:
                    averageTxt = value.lower()
                    averageTxt=averageTxt.replace("average","")
                    averageParts = averageTxt.split("/")
                    #print cols in df
                    df.columns = df.columns.str.strip()
                    df['AverageMarks'] = df[averageParts].mean(axis=1).round(2)
                    
                if "Numeric" in value:
                    numericCols = value.lower()
                    numericCols = numericCols.replace("converttonumeric","")
                    numericColsArr = numericCols.split("/")
                    #converting the columns into numeric
                    for col in numericColsArr:
                        if col in df.columns:
                            df[col] = pd.to_numeric(df[col], errors='coerce')

                # Modified deduplication logic - only apply if not skipping deduplication
                if "RemoveDuplicates" in value and not skip_deduplication:
                    print("Note: Deduplication will be handled globally across all chunks")
                    # Don't perform deduplication here - it will be handled globally

    return df

def format_if_date(value, output_format):
    try:
        # Try parsing the value as a date
        parsed = parse(str(value), fuzzy=False)
        # Return formatted date
        return parsed.strftime(output_format)
    except (ValueError, TypeError):
        # Not a date, return original value
        return value

class ExcelReader:
    def __init__(self, file_path):
        self.file_path = file_path

    def get_content(self):
        # Reads the Excel file and returns a DataFrame
        return pd.read_csv(self.file_path)

# Example usage
if __name__ == "__main__":
    # Define file paths
    input_file = "C:/Vincent/Asset Project Learning - python/CSVs/Input CSV/large_students_data.csv" 
    output_file = "C:/Vincent/Asset Project Learning - python/CSVs/Output CSV/output_file.csv" 
    rulescsv_path = "C:/Vincent/Asset Project Learning - python/CSVs/Rules/Rules.csv"  # Change this to your file path

    # Process the file
    process_csv_in_chunks(
        input_file=input_file,
        output_file=output_file,
        transform_function=data_transformation,
        chunk_size=50000  # Adjust based on your system's memory
    )
    
    print(f"Processing complete! Output saved to: {output_file}")