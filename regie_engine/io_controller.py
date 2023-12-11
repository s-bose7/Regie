import csv
import os
import pandas as pd
from pandas import DataFrame
from typing import List, Any
from datetime import date, datetime
from urllib.parse import urlparse,  ParseResult


class IOController:

    # static shared variables
    is_coulumn_inserted = False
    is_coulumn_inserted_in_stat = False
    is_coulumn_inserted_in_stat_history = False

    # Enter the input file name here
    input_file_name: str = "Regie_test.csv"
    
    output_file_name: str = "output"
    output_dir_name: str = "stats"
    output_file_path: str = ""
    
    # For live updates for monitoring
    stat_file_name: str = "email_collection_stats_latest.csv"
    stat_file_path: str = ""
    # For future reference and logging 
    stat_history_file_name: str = "email_collection_stats_prior.csv"
    stat_history_file_path: str = ""


    def __init__(self) -> None:

        try:
            os.makedirs(IOController.output_dir_name)
        except FileExistsError:
            pass

        IOController.output_file_path = os.path.join(
            IOController.output_dir_name, IOController.output_file_name
        )
        IOController.stat_file_path = os.path.join(
            IOController.output_dir_name, IOController.stat_file_name
        )
        IOController.stat_history_file_path = os.path.join(
            IOController.output_dir_name, IOController.stat_history_file_name
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
                df_column: List[str] = ["website", "email", "social link"]
                writer.writerow(df_column)
                IOController.is_coulumn_inserted = True

            writer.writerow(output_content)    


    @staticmethod
    def update_live_stat_for_thread(
        thread_id: int, 
        last_updated: datetime,
        total_url_passed: int,
        email_count: int, 
        social_link_count: int
    )-> None:
        stat_data: List[List[Any]] = []
        stat_df_column: List[str] = [
            "thread_id", "last_updated","total_url_passed", "email_found", "social_link_found"
        ]
        found = False
        if IOController.__file_already_exist(IOController.stat_file_path):
            IOController.is_coulumn_inserted_in_stat = True
            with open(IOController.stat_file_path, mode="r") as stat_o:
                reader = csv.reader(stat_o)
                stat_data = [row for row in reader] 
            
            for row in stat_data:
                if row and row[0].isdigit() and int(row[0]) == thread_id:
                    # Update statistics for existing thread
                    row[1] = last_updated
                    row[2] = total_url_passed
                    row[3] = email_count
                    row[4] = social_link_count
                    found = True
                    break
        
        if not found:
            # Add a new entry if the thread doesn't exist in stats
            stat_data.append([
                thread_id, 
                last_updated, 
                total_url_passed, 
                email_count, 
                social_link_count
            ])
        
        with open(IOController.stat_file_path, mode="w", newline="") as stat_o:
            writer = csv.writer(stat_o)
            if not IOController.is_coulumn_inserted_in_stat:
                writer.writerow(stat_df_column)
                IOController.is_coulumn_inserted_in_stat = True
            
            # live update
            writer.writerows(stat_data)
    

    @staticmethod
    def store_stat_history()->None:
        data: List[List[Any]] = []
        if IOController.__file_already_exist(IOController.stat_file_path):
            with open(IOController.stat_file_path, "r") as stat_o:
                reader = csv.reader(stat_o)
                data = [row for row in reader]
                if len(data) > 0 and data[0][0] == "thread_id":
                    # truncating the column
                    data = data[1:]
                        
        if IOController.__file_already_exist(IOController.stat_history_file_path):
            IOController.is_coulumn_inserted_in_stat_history = True
        
        with open(IOController.stat_history_file_path, "a", newline="") as stat_history:
            writer = csv.writer(stat_history)
            if not IOController.is_coulumn_inserted_in_stat_history:
                stat_df_column: List[str] = [
                    "thread_id", "last_updated","total_url_passed", "email_found", "social_link_found"
                ]
                writer.writerow(stat_df_column)
                IOController.is_coulumn_inserted_in_stat_history = True
            
            writer.writerows(data)


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
    def read_input(col_name: str)-> List[str]:
        input_df: DataFrame = pd.read_csv(IOController.input_file_name)
        urls: List[str] = input_df[col_name].tolist()
        return urls
    

    @staticmethod
    def merge_results()->None:
        # Merge based on 'website' column
        df_1: DataFrame = pd.read_csv(IOController.input_file_name)
        df_2: DataFrame = pd.read_csv(IOController.output_file_path)
        merged_df = pd.merge(df_1, df_2, on='website', how='inner')
        # merged_df['website'].fillna('UNKNOWN', inplace=True)
        # Sort the merged DataFrame based on the order of 'df_1' in the 'website' column
        # merged_df['website'] = pd.Categorical(merged_df['website'], categories=df_1['website'])
        # merged_df = merged_df.sort_values('website')
        merged_df.to_csv(IOController.input_file_name+"_results.csv", index=False)

