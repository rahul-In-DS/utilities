import pandas as pd
import ast
import json
from collections import Counter
from tqdm import tqdm

# Enable tqdm for pandas
tqdm.pandas()

# Load main dataset
filtered_df = pd.read_csv("flipkart-di-android-prod_12_14_2025_12_18_2025.csv")

# Define function to extract package names safely
def extract_package_names(row):
    try:
        parsed = json.loads(row)
        if isinstance(parsed, list):
            return [app.get("packagename") for app in parsed if isinstance(app, dict) and "packagename" in app]
        return []
    except:
        return []


filtered_df["packageName"] = filtered_df["applicationsNamesList"].progress_apply(extract_package_names)
filtered_df.drop(columns=["applicationsNamesList"], inplace=True)

filtered_df.to_csv("flipkart_di_android_prod_12_14_2025_12_18_2025_package_names.csv", index=False)
