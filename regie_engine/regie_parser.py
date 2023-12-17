from typing import List, Any, Set, Dict

import re
import requests
import pdfplumber
import aiohttp

import pandas as pd
from pandas import DataFrame, Series
from bs4 import BeautifulSoup as Bs
from datetime import datetime
from threading import Lock

from io_controller import IOController
from crawlbase import CrawlBaseAPI
from fake_useragent import UserAgent, FakeUserAgentError


class Regie:
    # Follows single responsibility principle
    # Regex parser to find EMAILS from any WEBSITE/PDF url 
    
    def __new_user_agent(self) -> str:
        try:
            agents = UserAgent()
            return agents.random
        except FakeUserAgentError:
            pass
        
        return self.__suppot_user_agent
            
    def __init__(
        self, 
        thread_id: int,
        target_df: DataFrame, 
        instruction: Dict[str, Any],
    )->None:
        self.thread_id: int = thread_id
        self.target_df: DataFrame = target_df
        self.page_html: str = ""
        self.current_page_url: str = ""
        self.current_row: Series = pd.Series([])
        self.lock: Lock = Lock()
        self.instruction: Dict[str, Any] = instruction
        self.email_counter: int = 0
        self.social_link_counter: int = 0
        self.completed_urls: int = 0 
        self.num_req: int = 0 # browser session tracker
        self.suppot_user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.140 Safari/537.36 Edge/18.17720"


    def __email_parser(self, html_content: Any) ->List[Any]:
        if not isinstance(html_content, (str, bytes)):
            return []
        
        def email_is_junk(email: Any)-> bool:
            for junk_c in ["wix", "io", "wixpress", "sentry", "jpg", "png", "jpeg", "gif", "webp"]:
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
        if not isinstance(root_page_html, (str, bytes)):
            return "None"
        soup = Bs(root_page_html, 'html.parser')
        try:
            # Find anchor tags <a> containing 'contact' in href attribute
            contact_link = soup.find(
                name='a', 
                href=lambda href: href and 'contact' in href.lower()
            ).get("href")

            if not contact_link.startswith(("http://", "https://")):
                with self.lock:
                    domain = IOController.extract_domain(
                        url=self.current_page_url, 
                        truncate_scheme=False
                    )
                    contact_link = domain + contact_link

        except AttributeError:
            contact_link = "None"

        return contact_link

    
    def __social_link_finder(self, root_page_html: Any) ->str:
        if not isinstance(root_page_html, (str, bytes)):
            return "None"
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


    def __valid_url(self, url: str)->bool:
        if not isinstance(url, str):
            return False
        if not url.startswith(('http://', 'https://')):
            return False
        
        with self.lock:
            domain = IOController.extract_domain(url)
            if domain == "docs.google.com":
                return False

        return True


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
            pdf_file_path = IOController.pdf_file_path
            with open(pdf_file_path, "wb") as pdf:
                pdf.write(response.content)

        except Exception:
            success = 1

        return success


    def __run_pdf_extractor_service(self, url: str)-> None:
        email_regex = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
        with self.lock:
            with pdfplumber.open("temp_pdf_downloader_service.pdf") as pdf:
                text: str = ""
                for page in pdf.pages:
                    text += page.extract_text()
                emails = re.findall(email_regex, text)
                if emails:
                    self.email_counter += len(emails)
                    self.completed_urls += 1
                    emails = ", ".join(emails)
                    row = self.current_row.tolist().append(emails)
                    IOController.store_data(output_content=row)
                    IOController.console_log(args=[url, self.thread_id ,"pdf", [emails]])


    async def __fetch_html(self, url):
        async with aiohttp.ClientSession() as session:
            try:
                headers = {
                    "User-Agent": f"{self.__new_user_agent}"
                }
                async with session.get(url, headers=headers) as response:
                    if response.status == 200:
                        html = await response.text()
                        return html
                    else:
                        print(f"Failed to fetch URL: {url}. Status code: {response.status}")
            except aiohttp.ClientError as e:
                print(f"Error fetching URL: {url}. Error: {e}")
        
        return None
    

    async def __run_website_extractor_service(self, url: str)-> int:
        try:
            self.page_html = await self.__fetch_html(url)
            #TODO: parse email if any
            emails = self.__email_parser(self.page_html)
            
            if len(emails) == 0:
                # specify that there's no email found from the current page; 
                # we need to search for contact-us page and/or social page;
                return 0
            else:
                with self.lock:
                    self.email_counter += len(emails)
                    self.completed_urls += 1
                    IOController.console_log(args=[url, self.thread_id ,"email(s)", emails])
                    emails = ", ".join(emails)
                    row: List[str] = self.current_row.tolist()
                    row.append(emails), row.append("") 
                    IOController.store_data(output_content=row)
                    # specify that we found email(s) from the current page;
                    # store it and move on 
                    return 1

        except Exception as e:
            print(f"[thread_{self.thread_id}] An unexpected error occurred: {e}")
            return 0


    async def __do_run_service(self, url: str, type: str)-> None:
        # redirect to respective extractor service based on URL type
        if type == "pdf" and not self.instruction["ignore_pdf_urls"]: 
            self.__run_pdf_extractor_service(url) 
            return

        home_page_service_code: int = await self.__run_website_extractor_service(url)
        if home_page_service_code == 0:
            #TODO:Find /contact-us link
            contact_link = self.__contact_link_finder(root_page_html=self.page_html)
            #TODO: Find /facebook link
            social_link = self.__social_link_finder(root_page_html=self.page_html)

            contact_service_code: int = 0
            if contact_link != "None":
                contact_service_code = await self.__run_website_extractor_service(contact_link)
            
            if (
                contact_service_code == 0 and 
                social_link != "None" and
                not self.instruction["ignore_facebook_urls"]
            ): 
                # email not found from contact-us page
                # pass social link to crawlbaseAPI to find emails
                with self.lock:
                    self.social_link_counter += 1
                    row = self.current_row.tolist()
                    IOController.console_log(args=[url, self.thread_id ,"social_link", social_link])
                    row.append(""), row.append(social_link)
                    IOController.store_data(output_content=row)


    async def run_service(self):
        target_col = self.instruction["target_col_name"]
        for index, row in self.target_df.iterrows():
            #TODO: perform any checks if required
            self.num_req += 1
            self.page_html = ""
            url = row[target_col]
            self.current_page_url = url
            self.current_row = row
            if not self.__valid_url(url): 
                continue

            #TODO: Run type checking on the current URL
            type: str = self.__check_url_type(url)
            if type == "pdf" and not self.instruction["ignore_pdf_urls"]: 
                status = self.__run_pdf_downloader_service(url)
                # If download status failed due to server error, we'll move on
                if status == 1: 
                    continue

            #TODO: Implementation of the main extraction module
            await self.__do_run_service(url, type)
            with self.lock: 
                IOController.update_live_stat_for_thread(
                    thread_id=self.thread_id,
                    last_updated=datetime.now(),
                    total_url_passed=len(self.target_df),
                    email_count=self.email_counter,
                    social_link_count=self.social_link_counter,
                    completed_urls=self.completed_urls,
                    total_requests = self.num_req
                )