import pandas as pd
import json
from openai import OpenAI

# -----------------------------
# STEP 1: Load Raw Data
# -----------------------------
data = {
    "employee_id": [1, 2, 3, 4, 5],
    "age": [25, 30, 28, 45, 120],   # bad value
    "salary": [50000, 60000, 55000, -1000, 58000]  # bad value
}

df = pd.DataFrame(data)
print("\nRaw Data:")
print(df)

# -----------------------------
# STEP 2: Create Statistical Summary (LLM Context)
# -----------------------------
summary = df.describe().to_json()
print("\nData Summary Sent to LLM:")
print(summary)

# -----------------------------
# STEP 3: Call LLM to Suggest Rules
# -----------------------------
client = OpenAI(api_key="YOUR_API_KEY")

prompt = f"""
You are a data quality expert.

Based on the dataset summary below, suggest data quality rules.
Return STRICT JSON only.

Format:
[
  {{ "column": "column_name", "min": value, "max": value }}
]

Dataset summary:
{summary}
"""

response = client.chat.completions.create(
    model="gpt-4o-mini",
    messages=[{"role": "user", "content": prompt}],
    temperature=0
)

rules_json = response.choices[0].message.content
print("\nLLM Suggested Rules:")
print(rules_json)

rules = json.loads(rules_json)

# -----------------------------
# STEP 4: Apply Rules Deterministically
# -----------------------------
def validate_data(df, rules):
    errors = []

    for rule in rules:
        col = rule["column"]
        min_val = rule["min"]
        max_val = rule["max"]

        invalid_rows = df[
            (df[col] < min_val) | (df[col] > max_val)
        ]

        if not invalid_rows.empty:
            errors.append({
                "column": col,
                "invalid_rows": invalid_rows.to_dict(orient="records")
            })

    return errors

validation_errors = validate_data(df, rules)

# -----------------------------
# STEP 5: Pipeline Decision
# -----------------------------
print("\nValidation Results:")

if validation_errors:
    print("❌ Data Quality FAILED")
    for err in validation_errors:
        print(f"Column: {err['column']}")
        for row in err["invalid_rows"]:
            print(row)
else:
    print("✅ Data Quality PASSED")

# -----------------------------
# STEP 6: Final Pipeline Action
# -----------------------------
if validation_errors:
    print("\nPipeline Status: STOPPED (send alert / quarantine data)")
else:
    print("\nPipeline Status: PROCEED (load to warehouse)")
