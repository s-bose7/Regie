import asyncio
from typing import List, Dict, Tuple
from regie_parser import Regie 
from pandas import DataFrame

from io_controller import IOController


async def runner_service(id: int, urls: DataFrame, bound: Tuple[int])-> None:
    print(f"Running service for thread_id: {id}")
    urls = urls.iloc[bound[0]:bound[1]]
    # Pass custom instructions
    instruction: Dict = {
        "target_col_name": "website",
        "ignore_pdf_urls": False,
        "ignore_facebook_urls": False,
    }
    parser = Regie(
        thread_id=id,
        target_df=urls,
        instruction=instruction
    )
    await parser.run_service()


async def main():
    io_controller = IOController()
    urls: DataFrame = io_controller.read_input(io_controller.input_file_name)
    NUM_THREAD = 4
    offset_u = len(urls)//NUM_THREAD
    tasks: List = []
    for i in range(NUM_THREAD):
        start_index = i * offset_u
        stop_index = start_index + offset_u 
        tasks.append(runner_service(i, urls, (start_index, stop_index)))

    await asyncio.gather(*tasks)
    IOController.store_stat_history()
    print("\nREGIE: All tasks have been joined\n")


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(main())
    finally:
        loop.close()

