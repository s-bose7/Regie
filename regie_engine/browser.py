import os
import logging
from dotenv import load_dotenv
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from fake_useragent import UserAgent, FakeUserAgentError
load_dotenv()

class Browser:

    def __init__(self, is_headless) -> None:
        self.__nature = is_headless
        self.__path = os.getenv('DRIVER_PATH')
        self.__suppot_user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.140 Safari/537.36 Edge/18.17720"
    
    
    def __new_user_agent(self) -> str:
        try:
            agents = UserAgent()
            return agents.random
        except FakeUserAgentError:
            logging.error("::FakeUserAgentError::\n")
        
        return self.__suppot_user_agent
    
    
    def get_driver_path(self)-> str:
        return self.__path
    
    
    def set_driver_path(self, path: str)->None:
        self.__path = path

    
    def set_up_browser(self):
        options = webdriver.ChromeOptions()
        if self.__nature: options.add_argument('--headless')
        options.add_argument(f"--{self.__new_user_agent()}")
        options.add_argument('--log-level=3')
        options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_service = Service(self.__path)
        chrome = webdriver.Chrome(options=options, service=chrome_service)
        self.chrome = chrome
        return self.chrome
    