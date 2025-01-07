

from queue import Queue, Empty, Full
import threading
from AWS import AWS_File_Downloader


class Downloader(threading.Thread):
    
    def __init__(self, in_queue: Queue, out_queue: Queue, target_path_constructor, result_path_constructor, *args, **kwargs):
        print('Downloader_init!')
        self.__in_queue = in_queue
        self.__out_queue = out_queue

        self.__AWS_File_Downloader = AWS_File_Downloader('coastal-imagery')
        self.__target_path_constructor = target_path_constructor
        self.__result_path_constructor = result_path_constructor
        
        self.__stop_event = threading.Event()

        super().__init__(*args, **kwargs)

    def stop(self):
        self.__stop_event.set()

    def run(self):
        print('Downloader_start!')
        while(True): # run loop
            if self.__stop_event.is_set():
                break # break out of run loop

            try:
                # Pull a file from the in queue, breaking if there is no work for 5 second
                # This makes sure we stop in 5 second if the stop event is set
                file_date_time: str = self.__in_queue.get(block=True, timeout=5)
                print(f'Thread {self.name} downloading {file_date_time}...')

                # if work we make use the generic functions to construct the path in the bucket
                # and the path to place the file on this machine.
                target_path = self.__target_path_constructor(file_date_time)
                result_path = self.__result_path_constructor(file_date_time)

                # initiate the actual file download
                self.__AWS_File_Downloader.download_file(target_path, result_path)

                # The file will need to be parsed and compressed by other workers
                # we place the file in the out queue for them blocking for 5 seconds 
                # making sure we can stop if the stop even is set
                item_held = True
                while(item_held): # put loop
                    if self.__stop_event.is_set():
                            break # break out of put loop

                    try:
                        self.__out_queue.put(file_date_time, True, timeout= 5)
                        item_held = False
                    except Full:
                        pass
            
             # We catch if the in_queue was empty down here so there we never try to put an empty string in the out queue
            except Empty:
                pass


