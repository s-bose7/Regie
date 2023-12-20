from typing import List

import requests
import threading
import pandas as pd
from pandas import DataFrame

import io_controller


class CrawlBaseAPI:
    #NOTE: Link to docs - https://crawlbase.com/docs/scraper-api/
    TOKEN = "tntnsjKU1IIKiYIXlZZrqQ"
    ENDPOINT = "https://api.crawlbase.com/scraper"
    
    def __init__(self) -> None:
        self.new_rows: List = []
        self.output_df: DataFrame = io_controller.IOController.read_input(
            file_path=io_controller.IOController.output_file_name
        )
        self.final_output_df = pd.DataFrame(columns=self.output_df.columns) 


    def __check_for_emails(self, facebook_url: str)->str:
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
            return ""
        
        return email


    def __email_finder(
        self,
        thread_id: int, 
        targeted_df: DataFrame, 
        exit_event: threading.Event
    )->None:
        print(f"Running crawlbase service for thread_id: {thread_id}")
        lock = threading.Lock()
        for index, row in targeted_df.iterrows():
            fb_page_url = row["social_handle"]
            if pd.isna(fb_page_url) or fb_page_url == "":
                self.new_rows.append(row)
                continue
            
            email: str = ""
            if pd.isna(row["emails"]) or row["emails"] == "":
                email = self.__check_for_emails(fb_page_url)
            
            if len(email) == 0:
                self.new_rows.append(row)
                continue
            
            row["emails"] = email
            self.new_rows.append(row)
            print(f"[thread_{thread_id}] [{email}] found for url {fb_page_url}")


    def run_crawlbase_service(self)->None:
        task_queue: List = []
        exit_event = threading.Event()
        num_threads = 17
        offset = len(self.output_df)//num_threads
        for thread_id in range(num_threads):
            start = thread_id * offset
            stop = start + offset
            thread = threading.Thread(
                target=self.__email_finder, 
                args=(thread_id, self.output_df.iloc[start:stop], exit_event)
            )
            thread.start()
            task_queue.append(thread)

        # Wait for all threads to finish
        for thread in task_queue:
            thread.join()
        
        self.final_output_df = self.final_output_df.append(self.new_rows, ignore_index=True)
        self.final_output_df.to_csv("final_output_regie.csv")
        print("\nCRAWLBASE: All tasks have been joined.\n")

