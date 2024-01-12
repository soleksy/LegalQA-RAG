import os
import json
import dotenv
import logging
import datetime

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager


dotenv.load_dotenv()
LOGIN_URL=os.getenv('LOGIN_URL')
LOGOUT_URL=os.getenv('LOGOUT_URL')
REDIRECT_URL=os.getenv('REDIRECT_URL')
REFERER_URL=os.getenv('REFERER_URL')


class SessionManager:
    def __init__(self, email: str, password: str) -> None:
        self.email = email
        self.password = password

        with open('sessionmanager/config.json') as f:
            config = json.load(f)
        
        self.config = config
        self.driver = self.setup_driver()

        self.login_url = LOGIN_URL
        self.redirect_url = REDIRECT_URL

        self.cookies = {}
        self.headers = {}
        self.date = str(datetime.datetime.now()).split()[0]

        try:
            self.login()
            self.check_session()
            self.check_resources_page()
            self.collect_session_data()
        except Exception as e:
            self.handle_exception(e)

    def setup_driver(self) -> webdriver.Chrome:
        chrome_options = Options()
        for argument in self.config['chrome_options']:
            chrome_options.add_argument(argument)

        service = Service(ChromeDriverManager().install())
        return webdriver.Chrome(service=service, options=chrome_options)

    def wait_for_element(self, by: By, value: str , timeout: float) -> WebDriverWait:
        return WebDriverWait(self.driver, timeout).until(EC.presence_of_element_located((by, value)))

    def login(self) -> None:
        self.driver.get(self.login_url)
        self.wait_for_element(By.ID, 'login_btn', self.config['timeout'])
        self.driver.find_element(By.NAME, 'login').send_keys(self.email)
        self.driver.find_element(By.NAME, 'password').send_keys(self.password)
        self.driver.find_element(By.ID, 'login_btn').click()

    def check_session(self) -> None:
        try:
            self.wait_for_element(By.CLASS_NAME, 'ui-dialog-title', self.config['timeout'])
            self.driver.find_element(By.CSS_SELECTOR, '.ui-button.ui-widget.ui-state-default.ui-corner-all.ui-button-text-only').click()
            self.driver.find_element(By.CLASS_NAME, 'release-access-action').click()
        except TimeoutException:
            pass
        finally:
            self.driver.get(self.redirect_url)

    def check_resources_page(self) -> None:
        self.wait_for_element(By.CLASS_NAME, 'result-text', self.config['timeout'])

    def collect_session_data(self) -> None:
        cookies = self.driver.get_cookies()
        for cookie in cookies:
            if cookie['name'] in self.config['required_cookies']:
                self.cookies[cookie['name']] = cookie['value']

        self.headers = self.config['headers_template']
        self.headers['Referer'] = REFERER_URL
        self.headers['X-XSRF-TOKEN'] = self.cookies.get('XSRF-TOKEN', '')

    def handle_exception(self, exception: Exception) -> None:
        logging.error(f"An error occurred: {exception}")
        if self.config['take_screenshot_on_error']:
            self.driver.save_screenshot(f"sessionmanager/error_{self.date}.png")
        self.driver.quit()
        raise exception

    def get_cookies(self) -> dict:
        return self.cookies

    def get_headers(self) -> dict:
        return self.headers

    def logout(self) -> None:
        self.driver.get(LOGOUT_URL)
        self.driver.quit()
