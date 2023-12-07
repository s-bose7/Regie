import csv
import os
import pandas as pd
from pandas import DataFrame, Series
from typing import List
from datetime import date, datetime
from urllib.parse import urlparse, ParseResult


class IOController:

    # static shared variables
    output_df: DataFrame = None 
    is_coulumn_inserted_in_stat = False
    # pass the input file name
    input_file_name: str = "Regie_test.csv"
    stat_file_name: str = "email_collection_stats.csv"


    def __init__(self) -> None:
        IOController.output_df = IOController.read_input()
        IOController.output_df["emails"] = None
        IOController.output_df["social_links"] = None


    @staticmethod
    def __file_already_exist(file_path: str)-> bool:
        try:
            with open(file_path, "r") as f_handle:
                reader = csv.reader(f_handle)
                return True
        
        except FileNotFoundError:
            return False


    @staticmethod
    def store_data(index: int, content: List[str])-> None:
        IOController.output_df.at[index, "emails"] = content[0]
        IOController.output_df.at[index, "social_links"] = content[1]
    
    
    @staticmethod
    def write_csv():
        if IOController.output_df is not None:
            IOController.output_df.to_csv(f"regie_results_run_{date.isoformat(datetime.now())}.csv")


    @staticmethod
    def update_stat_for_thread(
        thread_id: int, 
        timestamp: datetime,
        total_url_passed: int,
        email_count: int, 
        social_link_count: int
    )-> None:
        if IOController.__file_already_exist(IOController.stat_file_name):
            IOController.is_coulumn_inserted_in_stat = True
        with open(IOController.stat_file_name, mode="a", newline="") as stat_o:
            writer = csv.writer(stat_o)
            if not IOController.is_coulumn_inserted_in_stat:
                stat_df_column: List[str] = ["thread_id", "timestamp","total_url_passed", "email_found", "social_link_found"]
                writer.writerow(stat_df_column)
                IOController.is_coulumn_inserted_in_stat = True
            
            writer.writerow([thread_id, timestamp,total_url_passed, email_count, social_link_count])


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
    def read_input()-> DataFrame:
        try:
            input_df: DataFrame = pd.read_csv(
                filepath_or_buffer=IOController.input_file_name
            )
            return input_df
        except FileNotFoundError:
            print("Input file not found or path is incorrect.\n") 
            return