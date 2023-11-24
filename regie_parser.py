import time
import re
import logging
import threading

from browser import Browser
from io_controller import IOController

from typing import List, Any
from bs4 import BeautifulSoup as Bs
from urllib3.exceptions import MaxRetryError
from urllib3.exceptions import NewConnectionError


# REGEX PARSER TO FIND EMAILS/CONTACT DETAILS FROM ANY WEBSITE/PDF Doc
class Regie:
    
    def __init__(self, thread_id: int, target_urls: List) -> None:
        self.thread_id = thread_id
        self.target_urls = target_urls
        self.email_counter = 0
        self.social_link_counter = 0
        self.browser = self.__set_up_driver_browser()
        self.lock = threading.Lock()
        self.io = IOController()


    def __set_up_driver_browser(self):
        # utility service 
        browser = Browser(is_headless=False).set_up_browser()
        return browser
    

    def __get_response_with_retries(self, url: str) -> None:
        MAX_RETRY = 5
        RETRY_AFTER = 5
        for TRY in range(MAX_RETRY):
            try:
                self.browser.get(url)
                time.sleep(5.0)
                self.__close_unwanted_tabs_if_any(self.browser)
                return
            
            except (NewConnectionError, MaxRetryError) as e:
                print("\n")
                logging.error(msg=f"Failed to get response from {url}\n")
                time.sleep(RETRY_AFTER)
                continue


    def __close_unwanted_tabs_if_any(self, browser):
        # Close unwanted tabs
        main_window = browser.current_window_handle
        for handle in browser.window_handles:
            if handle != main_window:
                browser.switch_to.window(handle)
                browser.close()

        # Switch back to the main window
        browser.switch_to.window(main_window)
    

    def __parse_text_from_html(self)-> str:
        html = self.browser.page_source
        bs = Bs(html, 'html.parser')
        text = bs.get_text()
        return text


    def __email_parser(self, page_text: str) ->str:
        '''
        Special cases involving: 
            Complex local parts, 
            Obscure address formats,
            Internationalized domain names
        '''
        def non_ws(s) -> str: 
            return s.replace("\n", "").replace(" ", "")
        
        email_pattern = r'[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,6}'
        emails_found: List[Any] = re.findall(email_pattern, non_ws(page_text))
        return emails_found


    def __contact_link_finder(self, root_page_text: str) ->str:
        return "None"

    
    def __social_link_finder(self, root_page_text: str) ->str:
        return "None"


    def __do_run_service(self, url):
        self.__get_response_with_retries(url=url)

        #TODO: get the text from the current page
        page_text = self.__parse_text_from_html()
        #TODO: parse email if any
        email = self.__email_parser(page_text=page_text)

        if email == "None":
            #TODO: if email not found, find contact us links
            contact_link = self.__contact_link_finder(root_page_text=page_text)
            if contact_link == "None":

                #TODO: if contact us linkes not found, find facebook links
                social_link = self.__social_link_finder(root_page_text=page_text)
                #TODO: if found export
                if social_link != "None": 
                    self.social_link_counter += 1
                    self.__console_log(args=[url, "social_link", social_link])
                    row: List[str] = [url, "", social_link]
                    self.io.store_data(output_content=row)
                else:
                    # move on probably
                    self.io.export_failed_result(url=url)
            
            else:
                self.__do_run_service(url=contact_link)
        
        else:
            self.email_counter += 1
            self.io.console_log(args=[url, self.thread_id ,"email", email])
            row: List[str] = [url, email, ""]
            self.io.store_data(output_content=row)


    def run_service(self):
        for url in self.target_urls:
            #TODO: perform any checks if required
            if isinstance(url, str):
                self.__do_run_service(url)
            else:
                logging.error(f"Inappropriate type passed: {type(url)}, should be string")
                self.io.export_failed_result(url=url)
                # raise TypeError
                continue
            
        # target reached for thread: thread_id: int
        self.io.update_stat_for_thread(
            thread_id=self.thread_id,
            total_url_passed=len(self.target_urls),
            email_count=self.email_counter,
            social_link_count=self.social_link_counter
        )