import threading
from typing import List, Dict
from regie_parser import Regie 
from pandas import DataFrame
from io_controller import IOController


def runner_service(id, target_urls: DataFrame)-> None:
    print(f"Running service for thread_id: {id}")
    # Enter your custom changes on URL type here 
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
    # pass the coulmn name that has the input urls; i.e. "website", "urls", "links"
    urls: DataFrame = io_controller.read_input()
    NUM_THREAD: int = 2
    # Enter how many links should each thread take
    offset_u: int = int(len(urls)/NUM_THREAD)
    all_threads: List[threading.Thread] = []
    for thread in range(NUM_THREAD):
        start_indx: int = thread * offset_u
        stop_indx: int = start_indx + offset_u
        thread = threading.Thread(
            target=runner_service, 
            args=(thread, urls.iloc[start_indx:stop_indx])
        )
        thread.start()
        all_threads.append(thread)

    for therad in all_threads:
        therad.join()
    
    IOController.write_csv()
    print("\nALL THREADS HAVE BEEN JOINED, ENDCODE 0\n")
