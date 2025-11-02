import pandas as pd
import json

def convert_period_to_date_range(year, quarter):
    quarter_mapping = {
        "Q1": ("01-01", "03-31"),
        "Q2": ("04-01", "06-30"),
        "Q3": ("07-01", "09-30"),
        "Q4": ("10-01", "12-31")
    }
    if quarter not in quarter_mapping:
        return None, None
    
    start_suffix, end_suffix = quarter_mapping[quarter]
    return f"{year}-{start_suffix}", f"{year}-{end_suffix}"

input_path = 'input_data_technical.txt'
output_path = 'processed_data.csv'

with open(input_path, 'r', encoding='utf-8') as file:
    json_data = json.load(file)

raw_records = json_data["financialGrowth"]

processed_rows = []

for record in raw_records:
    row = record.copy()

    if 'period' in row and 'calendarYear' in row:
        start, end = convert_period_to_date_range(row['calendarYear'], row['period'])
        row['start_date'] = start
        row['end_date'] = end
        row['date'] = start
    else:
        row['start_date'] = row.get('date')
        row['end_date'] = row.get('date')

    processed_rows.append(row)

df = pd.DataFrame(processed_rows)
df.sort_values(by=['symbol', 'date'], inplace=True)
df.drop_duplicates(subset=['symbol', 'date'], keep='last', inplace=True)

df.to_csv(output_path, index=False)

print(f" 處理成功！輸出檔案位於：{output_path}")
