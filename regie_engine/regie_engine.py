import threading
from typing import List, Dict
from regie_parser import Regie 
from io_controller import IOController

#NOTE: Always export/remove the 'stats/output.csv' from 'stats' folder before running a new dataset

def runner_service(id, target_urls: List[str])-> None:
    print(f"Running service for thread_id: {id}")
    # Pass custom instructions
    instruction: Dict = {
        "ignore_pdf_urls": False,
        "ignore_facebook_urls": False,
    }
    parser = Regie(
        thread_id=id, 
        target_urls=target_urls,
        instruction=instruction,
    )
    parser.run_service()


if __name__ == "__main__":
    io_controller: IOController = IOController()
    # pass the column name in the input df, where URLs can be found
    urls: List[str] = io_controller.read_input(col_name="website")
    NUM_THREAD: int = 4
    # could left out few URLs if not set 'manually'.
    offset_u: int = int(len(urls)/NUM_THREAD)
    all_threads: List[threading.Thread] = []
    for thread in range(NUM_THREAD):
        start_indx: int = thread * offset_u
        stop_indx: int = start_indx + offset_u
        thread = threading.Thread(target=runner_service, args=(thread, urls[start_indx:stop_indx]))
        thread.start()
        all_threads.append(thread)

    for therad in all_threads:
        therad.join()
    
    IOController.store_stat_history()
    IOController.merge_results()
    print("\nALL THREADS HAVE BEEN JOINED, ENDCODE 0\n")
