import threading
from typing import List, Dict
from regie_parser import Regie 
from io_controller import IOController


def runner_service(id, target_urls: List[str])-> None:
    print(f"Running service for thread_id: {id}")
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
    urls: List[str] = io_controller.read_input()
    NUM_THREAD: int = 2
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
    
    print("\nALL THREADS HAVE BEEN JOINED, ENDCODE 0\n")
