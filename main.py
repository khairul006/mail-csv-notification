import os
import psycopg2
import pandas as pd
import smtplib
from email.message import EmailMessage
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def get_data_from_pg():
    """Connects to Postgres and returns a DataFrame."""
    try:
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST"),
            database=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASS"),
            port=os.getenv("DB_PORT")
        )
        query = "SELECT * FROM transaction" # Use specific columns in production
        df = pd.read_sql_query(query, conn)
        conn.close()
        return df
    except Exception as e:
        print(f"Database Error: {e}")
        return None

def send_email(file_path):
    """Sends an email using Ethereal SMTP for testing."""
    msg = EmailMessage()
    msg['Subject'] = 'Database Export Test (Ethereal)'
    msg['From'] = os.getenv("EMAIL_USER")
    msg['To'] = os.getenv("EMAIL_RECEIVER")
    msg.set_content("This is a test email containing the database CSV export.")

    # Attach the CSV
    try:
        with open(file_path, 'rb') as f:
            msg.add_attachment(
                f.read(),
                maintype='text',
                subtype='csv',
                filename=os.path.basename(file_path)
            )
    except FileNotFoundError:
        print(f"Error: The file {file_path} was not found.")
        return

    try:
        # Use SMTP (587) + starttls() for Ethereal
        with smtplib.SMTP(os.getenv("EMAIL_HOST"), int(os.getenv("EMAIL_PORT"))) as server:
            server.starttls() # Secure the connection
            server.login(os.getenv("EMAIL_USER"), os.getenv("EMAIL_PASS"))
            server.send_message(msg)
            
        print("Success: Email captured by Ethereal!")
        print(f"Check your messages at: https://ethereal.email/messages")
        
    except Exception as e:
        print(f"SMTP Error: {e}")

def main():
    # 1. Fetch Data
    df = get_data_from_pg()
    
    if df is not None:
        # 2. Save to CSV
        csv_filename = "report_export.csv"
        df.to_csv(csv_filename, index=False)
        print(f"Data saved to {csv_filename}")

        # 3. Send Email
        send_email(csv_filename)
        
        # 4. Clean up (Optional: delete file after sending)
        # os.remove(csv_filename)

if __name__ == "__main__":
    main()