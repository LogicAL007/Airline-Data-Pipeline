import numpy as np
import pandas as pd

# Read the CSV file into a DataFrame
df = pd.read_csv('/mnt/c/Users/ayomi/OneDrive/Desktop/BIG_DATA/mydocumentstreaming/client/Clean_Dataset.csv'  )

# Convert the DataFrame to a JSON string and split it into lines
json_lines = df.to_json(orient='records', lines=True ).splitlines()

# Convert the JSON lines back into a DataFrame
dfjson = pd.read_json('\n'.join(json_lines), lines=True)

# Rename the 'Unnamed: 0' column to 'Serial No'
#dfjson = dfjson.rename(mapper={'Unnamed: 0': 'Serial No'}, axis=1)

# Save the DataFrame to a text file
np.savetxt('output32.txt', dfjson.values, fmt='%s')