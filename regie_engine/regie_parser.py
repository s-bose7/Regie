import re
import asyncio
import requests
import pdfplumber

from datetime import datetime
from typing import List, Any, Set, Dict
from bs4 import BeautifulSoup as Bs
from threading import Lock
from io_controller import IOController

from pyppeteer.browser import Browser
from pyppeteer.errors import PageError, TimeoutError, NetworkError


class Regie:
    # Follows single responsibility principle
    # Regex parser to find EMAILS from any WEBSITE/PDF url 
    def __init__(self, 
        thread_id: int,
        main_browser: Browser,
        target_urls: List, 
        instruction: Dict[str, bool],
    ) -> None:
        self.thread_id = thread_id
        self.target_urls = target_urls
        self.email_counter = 0
        self.social_link_counter = 0
        self.page_html = ""
        self.current_page_url = ""
        self.lock = Lock()
        self.instruction = instruction
        self.browser = main_browser


    def __email_parser(self, html_content: Any) ->List[Any]:
        if not isinstance(html_content, (str, bytes)):
            return []
        def email_is_junk(email: Any)-> bool:
            for junk_c in ["wix", "io", "wixpress", "sentry", "jpg", "png", "jpeg", "gif"]:
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
                domain = IOController.extract_domain(url=base_url, truncate_scheme=False)
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
        if IOController.extract_domain(url) == "docs.google.com":
            return False

        return True


    def __check_url_type(self, url: str)-> str:
        pdf = r"document"
        if re.search(pdf, url):
            return "pdf"

        return "website"


    async def __run_pdf_downloader_service(self, url: str)-> int:
        print("running pdf downloader service")
        success = 0
        try:
            response = requests.get(url)
            with open(IOController.pdf_file_path, "wb") as pdf:
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


    async def __fetch_html(self, url, page, retry=3):
        for i in range(retry):
            try:
                await page.goto(url, options={'waitUntil': 'domcontentloaded'})
                content = await page.content()
                return content
            
            except NetworkError as e:
                print(f"Network error occurred: {e}. Retrying...({i+1}/{retry})")
                await asyncio.sleep(1) 
            
            except TimeoutError as te:
                print(f"TimeoutError occurred: {url}")
                await asyncio.sleep(1)
            
            except Exception as e:
                print(f"An unexpected error occurred: {e}")
                break
        
        return None
    

    async def __run_website_extractor_service(self, url: str)-> int:
        try:
            page = await self.browser.newPage()
            self.page_html = await self.__fetch_html(url, page)
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
        
        except PageError as pe:
            print(f"PageError occurred: {url}")
            return 0
        
        finally:
            with self.lock:
                if page.isClosed() == False:
                    await page.close()


    async def __do_run_service(self, url: str, type: str)-> None:
        # redirect to respective extractor service based on URL type
        if type == "pdf" and not self.instruction["ignore_pdf_urls"]: 
            self.__run_pdf_extractor_service(url) 
            return

        home_page_service_code: int = await self.__run_website_extractor_service(url)
        if home_page_service_code == 0:
            #TODO:Find /contact-us link
            contact_link = self.__contact_link_finder(root_page_html=self.page_html, base_url=url)
            #TODO: Find /facebook link
            social_link = self.__social_link_finder(root_page_html=self.page_html)

            if contact_link != "None":
                contact_service_code = await self.__run_website_extractor_service(contact_link)
                if (
                    contact_service_code == 0 and 
                    social_link != "None" and
                    not self.instruction["ignore_facebook_urls"]
                ): 
                    # email not found from contact-us page
                    # we have a social link to check
                    social_service_code = await self.__run_website_extractor_service(social_link)
                    if social_service_code == 0: 
                        # No email in social page, probably due to a blockage from fb
                        # store social link for crawlbaseAPI
                        with self.lock:
                            self.social_link_counter += 1
                            row = [url, "", social_link]
                            IOController.store_data(output_content=row)
                else:
                    # email not found from contact-us page
                    # social link not found
                    return

            elif social_link != "None" and not self.instruction["ignore_facebook_urls"]:
                social_service_code = await self.__run_website_extractor_service(social_link)
                if social_service_code == 0: 
                    # No email in social page, probably due to a blockage from fb
                    # store social link for crawlbaseAPI
                    with self.lock:
                        self.social_link_counter += 1
                        row = [url, "", social_link]
                        IOController.store_data(output_content=row)
            else:
                # email not found from home page
                # There's no contact link and social link
                return


    async def run_service(self):
        for url in self.target_urls:
            #TODO: perform any checks if required
            self.page_html = ""
            self.current_page_url = url
            if not self.__valid_url(url):
                continue

            #TODO: Run type checking on the current URL
            type: str = self.__check_url_type(url)
            if type == "pdf" and not self.instruction["ignore_pdf_urls"]: 
                status = await self.__run_pdf_downloader_service(url)
                # If download status failed due to server error, we'll move on
                if status == 1: 
                    continue

            #TODO: Implementation of the main extraction module
            await self.__do_run_service(url, type)
            with self.lock:
                IOController.update_live_stat_for_thread(
                    thread_id=self.thread_id,
                    last_updated=datetime.now(),
                    total_url_passed=len(self.target_urls),
                    email_count=self.email_counter,
                    social_link_count=self.social_link_counter
                )