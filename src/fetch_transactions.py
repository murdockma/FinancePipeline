from dotenv import load_dotenv
import os
from pyppeteer import launch
import pandas_gbq
from datetime import datetime
import asyncio

# Load env variables
load_dotenv()
username = os.getenv('BANK_USERNAME')
password = os.getenv('BANK_PASSWORD')

def fetch_latest_dates(project='electric-cortex-289700', database='transactions', table='f_unified_transactions'):
    try:
        query = f'''
            SELECT 
                MAX(CASE WHEN account = 'checking' THEN d_date END) AS max_checking_date,
                MAX(CASE WHEN account = 'credit' THEN d_date END) AS max_credit_date
            FROM `{project}.{database}.{table}`
        '''
        df = pandas_gbq.read_gbq(query, project_id=project)
        checking_date = extract_date(df['max_checking_date'])
        credit_date = extract_date(df['max_credit_date'])
        return checking_date, credit_date
    except Exception as e:
        print(f"Error fetching transactions: {e}")
        return None, None

def extract_date(series):
    datetime_np = series.values[0]
    return datetime_np.astype(datetime).strftime('%m/%d/%Y')

async def login_and_download(page, checking_date):
    # Go to the login page
    await page.goto('https://wellsfargo.com')
    await page.click('div.ps-masthead-sign-on a.ps-sign-on-text')

    # Log in
    await page.waitForSelector('#j_username')
    await page.type('#j_username', username)
    await page.type('#j_password', password)
    await page.waitForSelector('[data-testid="signon-button"]', {'visible': True})
    await page.click('[data-testid="signon-button"]')
    try:
        await page.click('[data-testid="signon-button"]')
    except Exception as e:
        print(f"Error clicking sign-on button: {e}")

    # Handle OTP
    await page.waitForSelector('li.LineItemLinkList__lineItemLinkListItem___HHmyb button.Button__button___Jo8E3')
    await page.click('li.LineItemLinkList__lineItemLinkListItem___HHmyb button.Button__button___Jo8E3')
    await page.waitForSelector('#otp')
    user_input = input("Please enter the OTP value: ")
    await page.type('#otp', user_input)

    # Click the "Continue" button using XPath
    await page.waitForXPath('//button[span[text()="Continue"]]')
    buttons = await page.xpath('//button[span[text()="Continue"]]')
    if buttons:
        await buttons[0].click()
    else:
        print('Continue button not found')

    # Navigate to the download area
    await page.waitForXPath('//*[@id="S_ACCOUNTS"]/div/div/span')
    elements = await page.xpath('//*[@id="S_ACCOUNTS"]/div/div/span')
    if elements:
        await elements[0].click()
    else:
        print("Account section element not found")

    await asyncio.sleep(5)  # Ensure page has loaded

    await page.waitForXPath('//*[text()="Download Account Activity"]')
    elements = await page.xpath('//*[text()="Download Account Activity"]')
    if elements:
        await elements[0].click()
    else:
        print("Download Account Activity link not found")

    # Clear the date field and enter the new date
    await asyncio.sleep(3)
    await page.waitForSelector('#fromDate')
    await page.evaluate('''document.querySelector('#fromDate').value = '' ''')
    await page.type('#fromDate', checking_date)

    # Select the file format and download
    await page.waitForSelector('[data-testid="radio-fileFormat-commaDelimited"]')
    await page.click('[data-testid="radio-fileFormat-commaDelimited"]')
    await page.waitForSelector('[data-testid="download-button"]')
    await page.click('[data-testid="download-button"]')

    # Confirm the download
    await asyncio.sleep(3)
    await page.keyboard.press('Enter')
    await page.keyboard.press('Enter')
    await asyncio.sleep(2)
    await page.keyboard.press('Enter')
    await asyncio.sleep(10)


async def main():
    # Fetch dates from transactions table
    max_checking_date, max_credit_date = fetch_latest_dates()

    if not max_checking_date:
        print("Failed to fetch the latest dates. Exiting.")
        return

    # Set up download path
    download_path = os.path.join(os.getcwd(), 'data')
    os.makedirs(download_path, exist_ok=True)

    # Launch browser and start download
    browser = await launch(headless=False)
    context = await browser.createIncognitoBrowserContext()
    page = await context.newPage()

    await login_and_download(page, max_checking_date)
    
    await browser.close()

if __name__ == "__main__":
    asyncio.run(main())