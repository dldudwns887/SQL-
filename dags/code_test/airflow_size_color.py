import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException, TimeoutException, ElementClickInterceptedException, ElementNotInteractableException
import time
from airflow.hooks.S3_hook import S3Hook
from io import StringIO
from selenium.webdriver.chrome.options import Options

def get_li_texts(driver, xpath):
    ul_element = driver.find_element(By.XPATH, xpath)
    li_elements = ul_element.find_elements(By.TAG_NAME, 'li')
    return [li.text for li in li_elements]

def visit_website(urls):
    options = Options()
    options.add_experimental_option("excludeSwitches", ["enable-logging"])
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')

    remote_webdriver = 'http://remote_chromedriver:4444/wd/hub'
    size_options = []
    color_options = []

    for url in urls:
        driver = None
        try:
            driver = webdriver.Remote(command_executor=remote_webdriver, options=options)
            driver.get(url)
            time.sleep(2)
            print(f"Visited: {url}")

            button_clicked = False
            for n in range(31, 42):
                try:
                    xpath = f'//*[@id="root"]/div[{n}]/div/button'
                    button = driver.find_element(By.XPATH, xpath)
                    driver.execute_script("arguments[0].scrollIntoView(true);", button)
                    time.sleep(1)
                    button.click()
                    print(f"Clicked button at div[{n}] on {url}")
                    button_clicked = True

                    next_n = n + 2
                    try:
                        button_1_xpath = f'//*[@id="root"]/div[{next_n}]/div[1]/div[1]/div[1]/div[1]/button[1]'
                        button_2_xpath = f'//*[@id="root"]/div[{next_n}]/div[1]/div[1]/div[1]/div[1]/button[2]'

                        driver.find_element(By.XPATH, button_1_xpath).click()
                        print(f"Clicked button 1 at div[{next_n}] on {url}")
                        test_option_1 = get_li_texts(driver, f'//*[@id="root"]/div[{next_n}]/div[2]/ul')

                        try:
                            driver.find_element(By.XPATH, f'//*[@id="root"]/div[{next_n}]/div[2]/ul/li[1]').click()
                        except NoSuchElementException:
                            pass

                        driver.find_element(By.XPATH, button_2_xpath).click()
                        print(f"Clicked button 2 at div[{next_n}] on {url}")
                        test_option_2 = get_li_texts(driver, f'//*[@id="root"]/div[{next_n}]/div[2]/ul')

                    except NoSuchElementException:
                        try:
                            button_xpath = f'//*[@id="root"]/div[{next_n}]/div[1]/div[1]/div[1]/div[1]/button'
                            driver.find_element(By.XPATH, button_xpath).click()
                            print(f"Clicked button at div[{next_n}] on {url}")
                            test_option_1 = get_li_texts(driver, f'//*[@id="root"]/div[{next_n}]/div[2]/ul')
                            test_option_2 = ['0']
                        except NoSuchElementException:
                            print(f"No additional buttons found at div[{next_n}] on {url}")
                            test_option_1 = ["No_found"]
                            test_option_2 = ["No_found"]

                    print(f"test_option_1 for {url}: {test_option_1}")
                    print(f"test_option_2 for {url}: {test_option_2}")
                    size_options.append(test_option_1)
                    color_options.append(test_option_2)
                    break

                except (NoSuchElementException, TimeoutException):
                    continue
                except ElementClickInterceptedException:
                    print(f"Element click intercepted on {url} at div[{n}], trying next element.")
                    continue

            if not button_clicked:
                print(f"No button found on {url}")
                size_options.append(["No_found"])
                color_options.append(["No_found"])

        except Exception as e:
            print(f"Failed to visit {url} due to {e}")
            size_options.append(["No_found"])
            color_options.append(["No_found"])

        finally:
            if driver:
                driver.quit()

    return size_options, color_options

def read_s3_and_add_size_color():
    s3_hook = S3Hook(aws_conn_id='aws_s3')
    bucket_name = 'papalio-test-bucket'
    key = 'test_otto/products_with_size_color.csv'
    key2 = 'test_otto/new_links.csv'

    s3_client = s3_hook.get_conn()

    obj = s3_client.get_object(Bucket=bucket_name, Key=key)
    df = pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))

    obj2 = s3_client.get_object(Bucket=bucket_name, Key=key2)
    df_new_list = pd.read_csv(StringIO(obj2['Body'].read().decode('utf-8')))
    description_list = df_new_list['description'].tolist()

    size_options, color_options = visit_websites(description_list)
    for i, desc in enumerate(description_list):
        df.loc[df['description'] == desc, 'size_options'] = [size_options[i]]
        df.loc[df['description'] == desc, 'color_options'] = [color_options[i]]

    updated_csv = StringIO()
    df.to_csv(updated_csv, index=False, encoding='utf-8')
    s3_client.put_object(Bucket=bucket_name, Key=key, Body=updated_csv.getvalue())
    print("Updated data with size and color saved to S3")

def visit_websites(description_list):
    size_options = []
    color_options = []

    for sub_list in description_list:
        sizes, colors = visit_website([sub_list])
        size_options.extend(sizes)
        color_options.extend(colors)

    return size_options, color_options

def main():
    read_s3_and_add_size_color()

if __name__ == '__main__':
    main()
