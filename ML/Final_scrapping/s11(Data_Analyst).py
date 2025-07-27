from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import json


def main():

    # Start Chrome WebDriver
    driver = webdriver.Chrome()

    driver.get("https://www.naukri.com/")

    # Wait for the login button and click it
    wait = WebDriverWait(driver, 10)
    login_button = wait.until(EC.element_to_be_clickable((By.LINK_TEXT, "Login")))
    login_button.click()

    # Fill in login details
    time.sleep(2)  # Wait for the login form to appear
# Locate and enter username
    username_field = driver.find_element(By.XPATH, "//div[@class='form-row']/input[@type='text']")
    username_field.send_keys("vaibhavgaikwad@gmail.com")

# Locate and enter password
    password_field = driver.find_element(By.XPATH, "//div[@class='form-row']/input[@type='password']")
    password_field.send_keys("")

    # Locate and click the login button
    login_button = driver.find_element(By.XPATH, "//button[@type='submit' and contains(@class, 'loginButton')]")
    login_button.click()

    

    # Wait for the search input field to appear
    wait = WebDriverWait(driver, 10)
    search_field = wait.until(EC.presence_of_element_located((By.CLASS_NAME, "suggestor-input")))

    search_field.send_keys("data analyst jobs")

    # # Press ENTER to trigger search
    search_field.send_keys(Keys.RETURN)

    driver.get("https://www.naukri.com/data-analyst-jobs?k=data%20analyst%20jobs&nignbevent_src=jobsearchDeskGNB")
    #Wait for login to complete
    time.sleep(15)

    #https://www.naukri.com/data-analyst-jobs-2?k=data+analyst+jobs&nignbevent_src=jobsearchDeskGNB

    final = []
    time.sleep(5) 
    rows = driver.find_elements(By.CLASS_NAME, "srp-jobtuple-wrapper")

    print(len(rows))
    for row in rows:
        print(f"{row.text}")
        print("---------------------")
        final.append(row.text)

    for k in range(2,300):
        url = "https://www.naukri.com/data-analyst-jobs-" + str(k) + "?k=data+analyst+jobs&nignbevent_src=jobsearchDeskGNB"
        driver.get(url)
        time.sleep(10) 
        rows = driver.find_elements(By.CLASS_NAME, "srp-jobtuple-wrapper")

        for row in rows:
            print(f"{row.text}")
            print("---------------------")
            final.append(row.text)

    
    
    with open("Data_Analyst_data.json", "a") as file:
        json.dump(final, file)  # Saves as a JSON array

    driver.close()

if __name__ == '__main__':
    main()