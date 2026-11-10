# RetailPulse — Retail Sales Intelligence

RetailPulse is a Streamlit application for exploring retail transaction data. Upload a CSV or Excel file, map its columns, process the records with Apache PySpark RDD, and inspect sales performance through interactive pages.

## Features

- Upload CSV, XLSX and XLS files.
- Preview raw uploaded records and map columns to standard fields.
- Clean and standardise records through a PySpark RDD pipeline.
- Business overview and monthly sales trends.
- Product Intelligence: search products, review revenue, units, average unit price, order count, monthly trend, regional sales and transaction details.
- Region Sales: compare regions, inspect one region, review top products and download regional reports.
- Product/category and customer/region analysis.
- Data-quality audit and cleaned transaction explorer.
- Download cleaned transactions and regional summaries.
- Dark navy/blue interface.

## Project structure

```text
RetailPulse/
├── app.py
├── requirements.txt
├── packages.txt
├── environment.yml
├── README.md
├── .gitignore
├── .streamlit/
│   └── config.toml
└── .github/
    └── workflows/
        └── python-check.yml
```

## Run locally on Windows

1. Install Python 3.11 (64-bit) and Java 17 (required by PySpark).
2. Open PowerShell in this project folder.
3. Create and activate a virtual environment:

   ```powershell
   py -3.11 -m venv .venv
   .\.venv\Scripts\Activate.ps1
   ```

4. Install dependencies:

   ```powershell
   python -m pip install --upgrade pip
   pip install -r requirements.txt
   ```

5. Start the website:

   ```powershell
   streamlit run app.py
   ```

6. Open the local URL printed by Streamlit, usually `http://localhost:8501`.

## Run in Google Colab

Upload `app.py` to `/content/app.py`, install the dependencies with:

```python
!apt-get -qq update
!apt-get -qq install -y openjdk-17-jre-headless
!pip install -q -r requirements.txt
```

Then start Streamlit in the background:

```python
import subprocess, time

log = open("/content/streamlit.log", "w")
process = subprocess.Popen(
    ["streamlit", "run", "/content/app.py",
     "--server.port=8501",
     "--server.address=0.0.0.0",
     "--server.headless=true"],
    stdout=log, stderr=subprocess.STDOUT
)
time.sleep(8)
print("Server running:", process.poll() is None)
```

Use the Colab port proxy for port `8501` to open it in a browser tab. The Colab runtime must remain active; this is not permanent public hosting.

## Deploy with Streamlit Community Cloud

1. Push this repository to your GitHub account.
2. In Streamlit Community Cloud, create a new app.
3. Select this repository, branch `main`, and entry point `app.py`.
4. Deploy. The `requirements.txt`, `packages.txt`, and `.streamlit/config.toml` are included for dependency, Java and theme configuration.

**PySpark deployment note:** PySpark requires a compatible Java runtime. `packages.txt` requests OpenJDK 17 for Streamlit Community Cloud. Cloud platform limits may still affect Spark startup and memory for large files.

## Expected input data

RetailPulse is designed for transaction-level retail data. Common recognised fields include:

- Date / InvoiceDate
- Order ID / InvoiceNo
- Product / Description / SKU
- Quantity
- Unit Price
- Revenue / Sales Amount
- Customer ID
- Category
- Country / Region
- Cost (optional)

The app provides automatic column suggestions and lets the user confirm or change the mapping. For revenue, it can calculate `Quantity × Unit Price` if a revenue column is not supplied.

## Data and interpretation notes

- Upload only data you are authorised to process.
- Revenue is not profit. Profit is only estimated when a suitable cost field is mapped.
- Product specifications (brand, material, size, colour, etc.) are not fabricated; they must be present in the uploaded data.
- The cleaning pipeline excludes rows without valid non-negative revenue and removes exact duplicate standardised records. Review the cleaning logic for your organisation's treatment of refunds, cancellations, tax and returns.
- Use anonymised/sample data when sharing the repository publicly. Do not commit customer personal data, credentials, API keys or private datasets.

## Technology

Python · Streamlit · Apache PySpark RDD · Pandas · Plotly · Excel/CSV
