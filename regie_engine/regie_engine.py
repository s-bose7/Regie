import asyncio
from typing import List, Dict
from pyppeteer import launch
from regie_parser import Regie 
from pandas import DataFrame
from io_controller import IOController

#NOTE: Always export/remove the 'stats/output.csv' from 'stats' folder before running a new dataset


async def runner_service(id, target_urls, browser)-> None:
    print(f"Running service for thread_id: {id}")
    # Pass custom instructions
    instruction: Dict = {
        "ignore_pdf_urls": False,
        "ignore_facebook_urls": False,
    }
    parser = Regie(
        thread_id=id,
        main_browser=browser, 
        target_urls=target_urls,
        instruction=instruction,
    )
    await parser.run_service()


async def main():
    io_controller = IOController()
    urls = io_controller.read_input(col_name="website")
    browser = await launch()

    NUM_THREAD = 4
    offset_u = len(urls) // NUM_THREAD

    tasks: List = []
    for i in range(NUM_THREAD):
        start_index = i * offset_u
        stop_index = start_index + offset_u if i < NUM_THREAD - 1 else len(urls)
        target_urls = urls[start_index:stop_index]
        tasks.append(runner_service(i, target_urls, browser))

    await asyncio.gather(*tasks)
    IOController.store_stat_history()
    IOController.merge_results()
    await browser.close()
    print("\nALL TASKS HAVE BEEN JOINED, ENDCODE 0\n")


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(main())
    finally:
        loop.close()

