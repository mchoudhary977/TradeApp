import pandas as pd

# Provide the path to your Excel file
# csv_file_path = "C:\\Users\\mchou\\Downloads\\bankstatements\\0246206075_statement (1).csv"
csv_file_path = "C:\\Users\\mchou\\Downloads\\bankstatements\\kotak_statement_1.csv"

# Read Excel file into a DataFrame
df = pd.read_csv(csv_file_path,skiprows=12)[:-4]
df.columns = df.iloc[0]
df = df[1:]
df['bank'] = 'Kotak'

df = df['Unnamed: 0'][0]

df['#'] = df[0]

df.columns


df[1]
print("DataFrame from Excel:")
print(df)

df.columns