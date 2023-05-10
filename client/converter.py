import numpy as np
import pandas as pd

# Read the CSV file into a DataFrame
df = pd.read_csv('/mnt/c/Users/ayomi/OneDrive/Desktop/BIG_DATA/Clean_Dataset.csv', encoding='ISO-8859-1')

# Convert the DataFrame to a JSON string and split it into lines
df["json"] = df.to_json(orient='records', lines=True ).splitlines()

dfjson = df["json"]
# Convert the JSON lines back into a DataFrame
#dfjson = pd.read_json('\n'.join(json_lines), lines=True)

# Save the DataFrame to a text file
np.savetxt('./output2.txt', dfjson.values, fmt='%s')


