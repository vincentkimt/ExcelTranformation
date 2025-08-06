import pandas as pd
import os
import sys

def read_csv_file(file_path):
    """
    Read a CSV file from a specific location using pandas
    
    Args:
        file_path (str): Path to the CSV file
        
    Returns:
        pandas.DataFrame: DataFrame containing the CSV data, or None if error
    """
    try:
        # Check if file exists
        if not os.path.exists(file_path):
            print(f"Error: File '{file_path}' does not exist.")
            return None
        
        # Read the CSV file
        print(f"Reading CSV file from: {file_path}")
        df = pd.read_csv(file_path)
        
        # Display basic information about the data
        print(f"\nFile successfully loaded!")
        print(f"Data shape: {df.shape[0]} rows × {df.shape[1]} columns")
        print(f"Column names: {list(df.columns)}")
        
        # Show data types
        print(f"\nData types:")
        print(df.dtypes)
        
        # Show first few rows
        print(f"\nFirst 5 rows:")
        print(df.head())
        
        # Show basic statistics for numeric columns
        if df.select_dtypes(include=['number']).shape[1] > 0:
            print(f"\nBasic statistics for numeric columns:")
            print(df.describe())
        
        # Check for missing values
        missing_values = df.isnull().sum()
        if missing_values.sum() > 0:
            print(f"\nMissing values per column:")
            print(missing_values[missing_values > 0])
        else:
            print(f"\nNo missing values found.")
        
        return df
        
    except FileNotFoundError:
        print(f"Error: File '{file_path}' not found.")
        return None
    except pd.errors.EmptyDataError:
        print(f"Error: The file '{file_path}' is empty.")
        return None
    except pd.errors.ParserError as e:
        print(f"Error parsing CSV file: {e}")
        return None
    except Exception as e:
        print(f"Unexpected error: {e}")
        return None

def read_csv_with_options(file_path, **kwargs):
    """
    Read CSV file with custom options
    
    Args:
        file_path (str): Path to the CSV file
        **kwargs: Additional pandas read_csv parameters
        
    Returns:
        pandas.DataFrame: DataFrame containing the CSV data
    """
    try:
        print(f"Reading CSV with custom options from: {file_path}")
        df = pd.read_csv(file_path, **kwargs)
        
        print(f"Data loaded successfully with shape: {df.shape}")
        print(f"Columns: {list(df.columns)}")
        
        return df
        
    except Exception as e:
        print(f"Error reading CSV with options: {e}")
        return None

def main():
    """
    Main function to demonstrate CSV reading
    """
    # Method 1: Specify the exact file path
    file_path = r"C:\Users\YourUsername\Documents\data.csv"  # Windows path example
    # file_path = "/home/username/documents/data.csv"        # Linux/Mac path example
    # file_path = "./data/sample.csv"                        # Relative path example
    
    print("="*60)
    print("CSV FILE READER SCRIPT")
    print("="*60)
    
    # Basic CSV reading
    df = read_csv_file(file_path)
    
    if df is not None:
        # You can now work with the DataFrame
        print(f"\nDataFrame is ready for analysis!")
        print(f"You can now perform operations like:")
        print(f"- df.info() - Get detailed info about the DataFrame")
        print(f"- df.shape - Get dimensions")
        print(f"- df.columns - Get column names")
        print(f"- df['column_name'] - Access specific columns")
        
        # Example operations
        print(f"\nExample operations:")
        print(f"DataFrame info:")
        df.info()
        
    print("\n" + "="*60)
    print("ADVANCED OPTIONS EXAMPLE")
    print("="*60)
    
    # Examples of reading CSV with different options
    advanced_examples = [
        {
            'description': 'Read with semicolon delimiter',
            'options': {'sep': ';'}
        },
        {
            'description': 'Read specific columns only',
            'options': {'usecols': ['column1', 'column2']}  # Replace with actual column names
        },
        {
            'description': 'Read with custom header row',
            'options': {'header': 1}  # Use row 1 as header (0-indexed)
        },
        {
            'description': 'Read without header',
            'options': {'header': None}
        },
        {
            'description': 'Skip first 2 rows',
            'options': {'skiprows': 2}
        },
        {
            'description': 'Read only first 100 rows',
            'options': {'nrows': 100}
        },
        {
            'description': 'Handle missing values',
            'options': {'na_values': ['N/A', 'NULL', 'null', '']}
        },
        {
            'description': 'Specify data types',
            'options': {'dtype': {'column1': str, 'column2': int}}  # Replace with actual columns
        }
    ]
    
    # Uncomment the example you want to try
    # for example in advanced_examples:
    #     print(f"\n{example['description']}:")
    #     df_advanced = read_csv_with_options(file_path, **example['options'])

def interactive_csv_reader():
    """
    Interactive function to get file path from user
    """
    print("Interactive CSV Reader")
    print("-" * 20)
    
    while True:
        file_path = input("Enter the path to your CSV file (or 'quit' to exit): ").strip()
        
        if file_path.lower() == 'quit':
            print("Goodbye!")
            break
        
        if file_path:
            df = read_csv_file(file_path)
            if df is not None:
                print(f"\nWould you like to save this data to a new location? (y/n): ", end="")
                save_choice = input().lower()
                if save_choice == 'y':
                    output_path = input("Enter output file path: ").strip()
                    try:
                        df.to_csv(output_path, index=False)
                        print(f"Data saved to: {output_path}")
                    except Exception as e:
                        print(f"Error saving file: {e}")
        else:
            print("Please enter a valid file path.")

# Different ways to specify file paths
def path_examples():
    """
    Examples of different ways to specify file paths
    """
    print("\nFILE PATH EXAMPLES:")
    print("-" * 20)
    
    # Absolute paths
    print("Absolute paths:")
    print("Windows: r'C:\\Users\\YourName\\Documents\\data.csv'")
    print("Linux/Mac: '/home/username/documents/data.csv'")
    print("Mac: '/Users/username/Documents/data.csv'")
    
    # Relative paths
    print("\nRelative paths:")
    print("Same directory: 'data.csv'")
    print("Subdirectory: 'data/sample.csv'")
    print("Parent directory: '../data.csv'")
    print("Two levels up: '../../data/sample.csv'")
    
    # Using os.path.join for cross-platform compatibility
    print("\nCross-platform paths using os.path.join:")
    sample_path = os.path.join("data", "samples", "file.csv")
    print(f"os.path.join('data', 'samples', 'file.csv') = {sample_path}")

if __name__ == "__main__":
    # Show path examples
    path_examples()
    
    # Choose execution mode
    print("\nChoose execution mode:")
    print("1. Run with predefined path")
    print("2. Interactive mode")
    
    choice = input("Enter your choice (1 or 2): ").strip()
    
    if choice == "1":
        main()
    elif choice == "2":
        interactive_csv_reader()
    else:
        print("Invalid choice. Running with predefined path...")
        main()