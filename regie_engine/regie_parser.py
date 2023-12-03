import time
import re
import random
import logging
import requests
import pdfplumber

from datetime import datetime
from browser import Browser
from io_controller import IOController
from threading import Lock
from typing import List, Any, Set
from bs4 import BeautifulSoup as Bs

from requests.exceptions import ConnectionError, HTTPError
from urllib3.exceptions import MaxRetryError
from urllib3.exceptions import NewConnectionError
from selenium.common.exceptions import WebDriverException


class Regie:
    # Follows single responsibility principle
    # Regex parser to find EMAILS from any WEBSITE/PDF url 
    def __init__(self, 
        thread_id: int, 
        target_urls: List, 
    ) -> None:
        self.thread_id = thread_id
        self.target_urls = target_urls
        self.email_counter = 0
        self.social_link_counter = 0
        self.lock = Lock()
        self.page_html = ""
        self.current_page_url = ""
        self.browser = self.__set_up_driver_browser()


    def __set_up_driver_browser(self):
        # utility service 
        with self.lock:
            browser = Browser(is_headless=False).set_up_browser()
            return browser
    

    def __get_response_with_retries(self, url: str) -> int:
        MAX_RETRY = 5
        RETRY_AFTER = 5
        for TRY in range(MAX_RETRY):
            try:
                self.browser.get(url)
                time.sleep(random.uniform(2.39, 2.93))
                self.__close_unwanted_tabs_if_any(self.browser)
                return 200
            
            except (NewConnectionError, MaxRetryError) as e:
                logging.error(msg=f"Failed to get response from {url}\n")
                time.sleep(RETRY_AFTER)
                continue

            except WebDriverException as WE:
                logging.error(f"Something wrong with the website's server - {url}\n")
                IOController.export_failed_result(url=url)
                break

        return 500


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
        Special cases involving: Complex local parts,
        Obscure address formats, Internationalized domain names
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


    def __contact_link_finder(self, root_page_html: Any, base_url: str) ->str:
        soup = Bs(root_page_html, 'html.parser')
        try:
            # Find anchor tags <a> containing 'contact' in href attribute
            contact_link = soup.find(
                name='a', 
                href=lambda href: href and 'contact' in href.lower()
            ).get("href")
            
            if not contact_link.startswith(("http://", "https://")):
                domain = IOController.extract_domain(url=base_url, truncate_scheme=False)
                contact_link = domain + contact_link
        
        except AttributeError:
            contact_link = "None"

        return contact_link

    
    def __social_link_finder(self, root_page_html: Any) ->str:
        soup = Bs(root_page_html, 'html.parser')
        try:
            # Find anchor tags <a> containing 'facebook' in href attribute
            facebook_link = soup.find(
                name='a', 
                href=lambda href: href and 'facebook' in href.lower()
            ).get("href")

        except AttributeError:
            facebook_link = "None"
        
        return facebook_link


    def __get_status_code(self, url: str)->int:
        try:
            response = requests.get(url)
            return response.status_code
        except (ConnectionError, HTTPError):
            return 500


    def __valid_url(self, url: str)->bool:
        # url_format_regex = r"^((https?)://)?(([a-zA-Z0-9\.-]+\.)+[a-zA-Z]{2,})$"
        domain = IOController.extract_domain(url)
        # server_status_code = self.__get_status_code(url)
        if (
            isinstance(url, str) and 
            url.startswith(('http://', 'https://')) and
            # re.match(url_format_regex, url) and
            domain != "docs.google.com"
            # server_status_code == 200 
        ):
            return True
    
        return False
    

    def __check_url_type(self, url: str)-> str:
        pdf = r"document"
        if re.search(pdf, url):
            return "pdf"
        
        return "website"
    

    def __run_pdf_downloader_service(self, url: str)-> int:
        print("running pdf downloader service")
        success = 0
        try:
            response = requests.get(url)
            with open("temp_pdf_downloader_service.pdf", "wb") as pdf:
                pdf.write(response.content)

        except Exception:
            success = 1
        
        return success
    
    def __run_pdf_extractor_service(self, url: str)-> None:
        email_regex = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
        with pdfplumber.open("temp_pdf_downloader_service.pdf") as pdf:
            text: str = ""
            for page in pdf.pages:
                text += page.extract_text()
            
            emails = re.findall(email_regex, text)
            if emails:
                self.email_counter += len(emails)
                emails = [", ".join(emails)]
                emails.insert(0, url)
                IOController.store_data(output_content=emails)


    def __run_website_extractor_service(self, url: str)-> int:
        status_code = self.__get_response_with_retries(url=url)
        if status_code == 200:
            #TODO: get the text from the current page
            self.page_html = self.__parse_html()
            #TODO: parse email if any
            emails = self.__email_parser(self.page_html)

            if len(emails) == 0:
                # specify that there's no email found from the current page; 
                # we need to search for contact-us page and/or social page;
                return 0
            
            else:
                with self.lock:
                    self.email_counter += len(emails)
                    IOController.console_log(args=[url, self.thread_id ,"email", emails])
                    row: List[str] = [email for email in emails]
                    row = [", ".join(row)]
                    if "facebook" in url: row.insert(0, self.current_page_url), row.append(url)
                    else: row.insert(0, url), row.append("")
                    IOController.store_data(output_content=row)
                    # specify that we found email(s) from the current page;
                    # store it and move on 
                    return 1
        else:
            IOController.export_failed_result(url=url)
            # specify that website is not responsive; so move on
            return 2
    

    def __do_run_service(self, url: str, type: str)-> None:
        # redirect to respective extractor service based on URL type
        if type == "pdf": 
            self.__run_pdf_extractor_service(url) 
            return
        
        home_page_service_code: int = self.__run_website_extractor_service(url)
        if home_page_service_code == 0:
            #TODO:Find /contact-us link
            contact_link = self.__contact_link_finder(root_page_html=self.page_html, base_url=url)
            #TODO: Find /facebook link
            social_link = self.__social_link_finder(root_page_html=self.page_html)

            if contact_link != "None":
                contact_service_code = self.__run_website_extractor_service(contact_link)
                if contact_service_code == 0 and social_link != "None": 
                    # email not found from contact-us page
                    # we have a social link to check
                    social_service_code = self.__run_website_extractor_service(social_link)
                    if social_service_code == 0 or social_service_code == 2: 
                        # No email in social page, probably due to a blockage from fb
                        # store social link for crawlbaseAPI
                        with self.lock:
                            self.social_link_counter += 1
                            row = [url, "", social_link]
                            IOController.store_data(output_content=row)
                else:
                    # email not found from contact-us page
                    # social link not found
                    if contact_service_code == 0:
                        with self.lock: IOController.export_failed_result(url)

            elif social_link != "None":
                social_service_code = self.__run_website_extractor_service(social_link)
                if social_service_code == 0 or social_service_code == 2: 
                    # No email in social page, probably due to a blockage from fb
                    # store social link for crawlbaseAPI
                    with self.lock:
                        self.social_link_counter += 1
                        row = [url, "", social_link]
                        IOController.store_data(output_content=row)
            else:
                # email not found from home page
                # There's no contact link and social link
                with self.lock: IOController.export_failed_result(url)
                        

    def run_service(self):
        for url in self.target_urls:
            #TODO: perform any checks if required
            self.page_html = ""
            self.current_page_url = url
            if self.__valid_url(url):
                #TODO: Run type checking on the current URL
                type: str = self.__check_url_type(url)
                if type == "pdf": 
                    status = self.__run_pdf_downloader_service(url)
                    # If download status failed due to server error, we'll move on
                    if status == 1: 
                        continue
                
                #TODO: Implementation of the main extraction module
                self.__do_run_service(url, type)
                    
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

#TODO: issues
# 1. stat column getting written multiple times
# 2. stat file & pdf should be in output dir