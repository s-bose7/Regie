import time
import re
import logging

from datetime import datetime
from browser import Browser
from io_controller import IOController
from threading import Lock

from typing import List, Any, Set
from bs4 import BeautifulSoup as Bs
from urllib3.exceptions import MaxRetryError
from urllib3.exceptions import NewConnectionError
from selenium.common.exceptions import WebDriverException


class Regie:
    # REGEX PARSER TO FIND EMAILS/CONTACT DETAILS FROM ANY WEBSITE/PDF Doc
    def __init__(self, thread_id: int, target_urls: List) -> None:
        self.thread_id = thread_id
        self.target_urls = target_urls
        self.email_counter = 0
        self.social_link_counter = 0
        self.lock = Lock()
        self.browser = self.__set_up_driver_browser()

    def __set_up_driver_browser(self):
        # utility service 
        with self.lock:
            browser = Browser(is_headless=False).set_up_browser()
            return browser
    

    def __get_response_with_retries(self, url: str) -> None:
        MAX_RETRY = 5
        RETRY_AFTER = 5
        for TRY in range(MAX_RETRY):
            try:
                self.browser.get(url)
                time.sleep(2.5)
                self.__close_unwanted_tabs_if_any(self.browser)
                return
            
            except (NewConnectionError, MaxRetryError) as e:
                logging.error(msg=f"Failed to get response from {url}\n")
                time.sleep(RETRY_AFTER)
                continue

            except WebDriverException as WE:
                logging.error(f"Something wrong with the website's server - {url}\n")
                IOController.export_failed_result(url=url)
                break


    def __close_unwanted_tabs_if_any(self, browser):
        # Close unwanted tabs
        main_window = browser.current_window_handle
        for handle in browser.window_handles:
            if handle != main_window:
                browser.switch_to.window(handle)
                browser.close()

        # Switch back to the main window
        browser.switch_to.window(main_window)
    

    def __parse_html(self)-> str:
        html = self.browser.page_source
        return html
    
    def __non_ws(self, s) -> str: 
        return s.replace("\n", "").replace(" ", "")


    def __email_parser(self, html_content: Any) ->List[Any]:
        '''
        Special cases involving: 
            Complex local parts, Obscure address formats, Internationalized domain names
        '''
        def email_is_junk(email: Any)-> bool:
            for junk_c in ["wix", "io", "wixpress", "sentry", "jpg", "png", "jpeg"]:
                if junk_c in email:
                    return True
                     
            return False
        
        _pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
        emails_found = re.findall(_pattern, html_content)
        
        clean_emails: Set[Any] = set()
        if emails_found:
            clean_emails = {email for email in emails_found if not email_is_junk(email)}

        return list(clean_emails)


    def __contact_link_finder(self, root_page_html: Any) ->str:
        return "None"

    
    def __social_link_finder(self, root_page_html: Any) ->str:
        return "None"


    def __do_run_service(self, url: str):
        self.__get_response_with_retries(url=url)

        #TODO: get the text from the current page
        page_html = self.__parse_html()
        #TODO: parse email if any
        emails = self.__email_parser(page_html)

        if len(emails) == 0:
            #TODO: if email not found, find contact us links
            contact_link = self.__contact_link_finder(page_html)
            if contact_link == "None":

                #TODO: if contact us linkes not found, find facebook links
                social_link = self.__social_link_finder(page_html)
                #TODO: if found export
                if social_link != "None": 
                    with self.lock:
                        self.social_link_counter += 1
                        self.__console_log(args=[url, "social_link", social_link])
                        row: List[str] = [url, "", social_link]
                        IOController.store_data(output_content=row)
                else:
                    # move on probably
                    IOController.export_failed_result(url=url)
            
            else:
                self.__do_run_service(url=contact_link)
        
        else:
            with self.lock:
                self.email_counter += len(emails)
                IOController.console_log(args=[url, self.thread_id ,"email", emails])
                row: List[str] = [url]
                row.extend(emails)
                row.append("")
                IOController.store_data(output_content=row)


    def run_service(self):
        for url in self.target_urls:
            #TODO: perform any checks if required
            
            if isinstance(url, str) and url.startswith(('http://', 'https://')):
                self.__do_run_service(url)
            else:
                logging.error(f"Invalid URL: {url}")
                IOController.export_failed_result(url=url)
                # raise TypeError
                continue
            
        # target reached for thread: thread_id: int
        with self.lock:
            IOController.update_stat_for_thread(
                thread_id=self.thread_id,
                timestamp=datetime.now(),
                total_url_passed=len(self.target_urls),
                email_count=self.email_counter,
                social_link_count=self.social_link_counter
            )