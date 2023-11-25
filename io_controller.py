import csv
import io
import os
from typing import List
from datetime import datetime
from urllib.parse import urlparse

class IOController:

    # static shared variables
    is_coulumn_inserted = False
    is_coulumn_inserted_in_stat = False
    input_file_name = "input.txt"
    output_file_name = "output.csv"
    output_dir_name = "results_regie_run"
    output_file_path = ""
    failed_results_file_name = "regie_failed_results_run.csv"
    failed_results_file_path = ""
    stat_file_name = "email_collection_stats.csv"
    stat_file_path = ""


    def __init__(self) -> None:
        try:
            os.makedirs(IOController.output_dir_name)
        except FileExistsError:
            pass

        IOController.output_file_path = os.path.join(
            IOController.output_dir_name,
            IOController.output_file_name
        )
        IOController.stat_file_path = os.path.join(
            IOController.output_dir_name,
            IOController.stat_file_name
        )
        IOController.failed_results_file_path = os.path.join(
            IOController.output_dir_name,
            IOController.failed_results_file_name
        )


    @staticmethod
    def store_data(output_content: List[str])-> None:
        with open(IOController.output_file_path, mode="a", newline="") as file_o:
            writer = csv.writer(file_o)
            if not IOController.is_coulumn_inserted:
                df_column: List[str] = ["Url", "Email", "Social link"]
                writer.writerow(df_column)
                IOController.is_coulumn_inserted = True

            writer.writerow(output_content)    


    @staticmethod
    def update_stat_for_thread(
        thread_id: int, 
        timestamp: datetime,
        total_url_passed: int,
        email_count: int, 
        social_link_count: int
    )-> None:
        with open(IOController.stat_file_name, mode="a", newline="") as stat_o:
            writer = csv.writer(stat_o)
            if not IOController.is_coulumn_inserted_in_stat:
                stat_df_column: List[str] = ["thread_id", "timestamp","total_url_passed", "email_found", "social_link_found"]
                writer.writerow(stat_df_column)
                IOController.is_coulumn_inserted_in_stat = True
            
            writer.writerow([thread_id, timestamp,total_url_passed, email_count, social_link_count])
    

    @staticmethod
    def export_failed_result(url: str)-> None:
        with open(IOController.failed_results_file_path, mode="a", newline="") as failed_o:
            csv.writer(failed_o).writerow([url])
            

    @staticmethod
    def console_log(args: List)-> None:
        
        def extract_domain(url: str)-> str:
            parsed_url = urlparse(url)
            if parsed_url.netloc:
                return parsed_url.netloc
            else:
                return parsed_url.path.split('/')[0]
        
        domain = extract_domain(args[0])
        timestamp = datetime.now()
        print(f"[thread_id: {args[1]}] [timestamp: {timestamp}]: url:{domain}, {str(args[2]).upper()} found: {args[3]}")


    @staticmethod
    def __do_read_urls(file: io.TextIOWrapper)-> List[str]:
        url_strings = []
        for line_content in file.readlines():
            line_content = line_content.strip()
            if line_content.startswith("https://")  or line_content.startswith("http://"): 
                url_strings.append(line_content.strip())
        
        return url_strings


    @staticmethod
    def read_input()-> List[str]:
        try:
            urls: List[str] = []
            with open(IOController.input_file_name, mode='r', newline="") as file_i:
                urls.extend(IOController.__do_read_urls(file_i))

        except FileNotFoundError:
            print("File not found or path is incorrect.\n") 
            return
        
        return urls
