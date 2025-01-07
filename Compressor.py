

from queue import Queue, Empty, Full
import threading
from zipfile import ZipFile
from shutil import rmtree
from os import path

class Compressor(threading.Thread):
    
    def __init__(self, in_queue: Queue, out_queue: Queue, archive_path: str, *args, **kwargs):
        print("Compressor_init!")
        self.__in_queue = in_queue
        self.__out_queue = out_queue

        if not path.exists(archive_path):
            ZipFile(archive_path, 'w').close()

        self.__archive_path = archive_path
        
        self.__stop_event = threading.Event()

        super().__init__(*args, **kwargs)

    def stop(self):
        self.__stop_event.set()

    def run(self):
                
            while(True): # run loop
                if self.__stop_event.is_set():
                    break # break out of run loop

                try:

                    # Pull a file from the in queue, breaking if there is no work for 5 second
                    # This makes sure we stop in 5 second if the stop event is set
                    file_obj: dict = self.__in_queue.get(block=True, timeout=5)

                    for key in file_obj.keys(): # There will always be 1 key
                        file_date_time = key
                    selected_files = file_obj[file_date_time]
                    print(f'Thread {self.name} compressing {file_date_time}...')

                    with ZipFile(self.__archive_path, 'a') as archive:
                        for selected_file in selected_files:
                            archive.write(f'./{file_date_time}/{selected_file}')
                    rmtree(file_date_time)

                    self.__out_queue.put_nowait(file_date_time) # We put the file in the out queue for the next worker

                # We catch if the in_queue was empty down here so there we never try to put an empty string in the out queue
                except Empty:
                    pass
                # We catch if a subfile asked for wasn't there
                except FileNotFoundError:
                    print(f'File not found! {file_date_time}')
                    rmtree(file_date_time) # Still remove the DIR
                    pass

