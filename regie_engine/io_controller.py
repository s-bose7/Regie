import csv
import io
import os
from typing import List
from datetime import date, datetime
from urllib.parse import urlparse, ParseResult


class IOController:

    # static shared variables
    is_coulumn_inserted = False
    is_coulumn_inserted_in_stat = False
    is_coulumn_inserted_in_failed_results = False

    input_file_name: str = "Columbus_yelp_actor_run.csv"
    output_file_name: str = "output"
    output_dir_name: str = "results_regie_run"
    output_file_path: str = ""
    failed_results_file_name: str = "regie_failed_results_run"
    failed_results_file_path: str = ""
    stat_file_name: str = "email_collection_stats.csv"
    stat_file_path: str = ""


    def __init__(self) -> None:

        try:
            os.makedirs(IOController.output_dir_name)
        except FileExistsError:
            pass
        
        IOController.output_file_name += f"_{date.isoformat(datetime.now())}.csv"
        IOController.failed_results_file_name += f"_{date.isoformat(datetime.now())}.csv"

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
    def __file_already_exist(file_path: str)-> bool:
        try:
            with open(file_path, "r") as f_handle:
                reader = csv.reader(f_handle)
                return True
        
        except FileNotFoundError:
            return False


    @staticmethod
    def store_data(output_content: List[str])-> None:
        if IOController.__file_already_exist(IOController.output_file_path):
            IOController.is_coulumn_inserted = True
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
        if IOController.__file_already_exist(IOController.stat_file_path):
            IOController.is_coulumn_inserted_in_stat = True
        with open(IOController.stat_file_name, mode="a", newline="") as stat_o:
            writer = csv.writer(stat_o)
            if not IOController.is_coulumn_inserted_in_stat:
                stat_df_column: List[str] = ["thread_id", "timestamp","total_url_passed", "email_found", "social_link_found"]
                writer.writerow(stat_df_column)
                IOController.is_coulumn_inserted_in_stat = True
            
            writer.writerow([thread_id, timestamp,total_url_passed, email_count, social_link_count])
    

    @staticmethod
    def export_failed_result(url: str)-> None:
        if IOController.__file_already_exist(IOController.failed_results_file_path):
            IOController.is_coulumn_inserted_in_failed_results = True
        with open(IOController.failed_results_file_path, mode="a", newline="") as failed_o:
            writer = csv.writer(failed_o)
            if not IOController.is_coulumn_inserted_in_failed_results:
                writer.writerow(["Failed Urls"])
                IOController.is_coulumn_inserted_in_failed_results = True

            writer.writerow([url])


    @staticmethod
    def extract_domain(url: str, truncate_scheme: bool = True)-> str:
        domain: str = ""
        parsed_url: ParseResult = urlparse(url)
        if parsed_url.netloc: 
            domain += parsed_url.netloc
        else: 
            domain += parsed_url.path.split('/')[0]        
        if not truncate_scheme: 
            domain = parsed_url.scheme + "://" + domain
        return domain 


    @staticmethod
    def console_log(args: List)-> None:
        domain = IOController.extract_domain(args[0])
        timestamp = datetime.now()
        print(f"[thread_id: {args[1]}] [timestamp: {timestamp}]: url:{domain}, {str(args[2]).upper()} found: {args[3]}")


    @staticmethod
    def __do_read_urls(file: io.TextIOWrapper)-> List[str]:
        url_strings = []
        reader = csv.reader(file)
        for line_content in reader:
            if len(line_content) < 6: 
                continue 
            line_content = line_content[5].strip()
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
            print("Input file not found or path is incorrect.\n") 
            return
        
        return urls