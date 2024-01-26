import requests


class CrawlBaseAPI:
    #NOTE: Link to docs - https://crawlbase.com/docs/scraper-api/
    TOKEN = "tntnsjKU1IIKiYIXlZZrqQ"
    ENDPOINT = "https://api.crawlbase.com/scraper"

    @staticmethod
    def __check_for_emails(facebook_url: str)->str:
        email: str = ""
        try:
            response = requests.get(
                url=f"{CrawlBaseAPI.ENDPOINT}?token={CrawlBaseAPI.TOKEN}&javascript=true&url="+facebook_url
            )
            if response.status_code == 200:
                data = response.json()
                whereabouts = data["body"]["about"]
                email_and_garbage = [item for item in whereabouts if "@" in item]
                try:
                    email = email_and_garbage[1] if len(email_and_garbage[1]) < len(email_and_garbage[0]) else email_and_garbage[0]
                except IndexError:
                    pass

        except Exception:
            pass
        
        return email

    @staticmethod
    def email_finder(thread_id: int, fb_page_url: str)->str:
        print(f"Running crawlbase service for thread_id: {thread_id}")
        email = CrawlBaseAPI.__check_for_emails(fb_page_url)
        print(f"[thread_{thread_id}] [{email}] found for url {fb_page_url}")
        return email
        

