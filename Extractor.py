

from queue import Queue, Empty, Full
import threading
from zipfile import ZipFile, BadZipFile
from os import mkdir, remove
from glob import glob


class Extractor(threading.Thread):
    
    def __init__(self, in_queue: Queue, out_queue: Queue, get_selected_files, result_path_constructor, *args, **kwargs):
        print("Extractor_init!")
        self.__in_queue = in_queue
        self.__out_queue = out_queue

        self.__get_selected_files = get_selected_files
        self.__result_path_constructor = result_path_constructor
        
        
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
                file_date_time: str = self.__in_queue.get(block=True, timeout=5)
                print(f'Thread {self.name} extracting {file_date_time}...')

                # if work we make use the generic functions to construct the path to find the archive
                result_path = self.__result_path_constructor(file_date_time)

                # Extract all the files into a temp dir
                mkdir(file_date_time)
                with ZipFile(result_path, 'r') as zObject:
                    zObject.extractall(f'./{file_date_time}')
                remove(result_path) # delete the archive

                # # Delete any non selected files
                selected_files: list[str] = self.__get_selected_files(file_date_time)
                # for file in glob(f'./{file_date_time}/*'):
                #     if file not in selected_files:
                #         remove(file)

                # Construct the result obj
                results = {file_date_time: selected_files}

                # The file will need to be parsed and compressed by other workers
                # we place the file in the out queue for them blocking for 5 seconds 
                # making sure we can stop if the stop even is set
                item_held = True
                while(item_held): # put loop
                    if self.__stop_event.is_set():
                        break # break out of put loop

                    try:
                        self.__out_queue.put(results, True, timeout= 5)
                        item_held = False
                    except Full:
                        pass
            
             # We catch if the in_queue was empty down here so there we never try to put an empty string in the out queue
            except Empty:
                pass
            
            # We catch if a subfile asked for wasn't there
            except FileNotFoundError:
                print(f'File not found! {file_date_time}')
                pass

            except BadZipFile:
                print(f'Bad Zip File! {file_date_time}')
                pass

            except FileExistsError:
                print(f'File Exists! {file_date_time}')
                pass

