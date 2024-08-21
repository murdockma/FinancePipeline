This project is a template for automating the process of downloading, processing, and uploading your bank transaction data. It logs into a Wells Fargo bank account, downloads the latest transaction details, and processes them before sending everything over to BigQuery for easy analysis and visualization.

- Install the required packages:
```bash
pip install -r requirements.txt
```

- Create a .env file with your bank login info:
```bash
BANK_USERNAME=your_username
BANK_PASSWORD=your_password
```

- Fetch and process your transactions:
```bash
python fetch_transactions.py
python transaction_handler.py
```
