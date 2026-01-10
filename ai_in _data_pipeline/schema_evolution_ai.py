# Practical LLM Use Case in a Data Pipeline (Python)
# Scenario
# You receive raw JSON data from multiple sources:
# Unknown / evolving schema
# Possible PII
# Data quality issues
# Business users want readable metrics
# We’ll use an LLM to:
# Discover schema
# Detect PI
# Generate data quality rules
# Auto-generate SQL transformations
# Explain issues (observability)

# Architectural Flow:
# Raw Data (JSON / API)
#         ↓
# Schema Discovery (LLM)
#         ↓
# PII Classification (LLM)
#         ↓
# Data Quality Rules (LLM)
#         ↓
# SQL / PySpark Generation (LLM)
#         ↓
# Warehouse (BigQuery / Snowflake)
#Sample Raw Data 

sample_data = [
    {
        "cust_id": "C101",
        "email": "abc@gmail.com",
        "amount": 1200,
        "txn_date": "2024-12-01"
    },
    {
        "cust_id": "C102",
        "email": "xyz@yahoo.com",
        "amount": -500,
        "txn_date": "2024-12-02"
    }
]

#LLM clinet setup:
from openai import OpenAI
client = OpenAI(api_key="YOUR_API_KEY")

#Schema Discovery Using LLM

def discover_schema(data):
    prompt = f"""
    Analyze the following JSON data and return:
    - Column name
    - Data type
    - Business meaning

    Data:
    {data}
    """

    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": prompt}]
    )

    return response.choices[0].message.content

schema_info = discover_schema(sample_data)
print(schema_info)
