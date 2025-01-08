from Downloader import Downloader
from Extractor import Extractor
from Compressor import Compressor
from AWS import AWS_File_Downloader
from queue import Queue, Empty
from datetime import datetime
import SaveFile
import json


with open('config.json', 'r') as config_file:
    config = json.load(config_file)

DOWNLOADER_COUNT = config['DOWNLOADER_COUNT']
EXTRACTOR_COUNT = config['EXTRACTOR_COUNT']
FROM_SAVE_FILE = config['FROM_SAVE_FILE']
FROM_TIME = datetime.fromisoformat(config['FROM_TIME'].replace('Z', '+00:00'))
TO_TIME = datetime.fromisoformat(config['TO_TIME'].replace('Z', '+00:00'))


def construct_target_path(file_date_time: str):
    return f'HCPDataFiles/{file_date_time}.zip'

def construct_result_path(file_date_time: str):
    return f'Downloads/{file_date_time}.zip'

def get_selected_files(file_date_time: str):
    return [
        f'Armcrest2_{file_date_time}.tiff'
    ]



class AWSDownloader:
    def __init__(self):
        print('INIT')

        self.to_download = Queue()
        self.to_extract = Queue()
        self.to_compress = Queue()
        self.to_mark_finished = Queue()

        self.downloaders = [Downloader(self.to_download, self.to_extract, construct_target_path, construct_result_path) for _ in range(DOWNLOADER_COUNT)]
        self.extractors = [Extractor(self.to_extract, self.to_compress, get_selected_files, construct_result_path) for _ in range(EXTRACTOR_COUNT)]
        self.compressor = Compressor(self.to_compress, self.to_mark_finished, './result.zip')


        if FROM_SAVE_FILE:
            print('Loading job from save file!')
            self.date_time_dict = SaveFile.load('./save.json')
        else:
            print('Reading bucket for files.... this may take some time!')
            AWSFD = AWS_File_Downloader('coastal-imagery')
            date_time_list = AWSFD.get_date_time_list(FROM_TIME, TO_TIME)
            print(f'{len(date_time_list)} files targeted!')

            self.date_time_dict = {date_time: False for date_time in date_time_list}
            SaveFile.save('./save.json', self.date_time_dict)


        for date_time, isDownloaded in self.date_time_dict.items(): 
            if not isDownloaded:
                self.to_download.put_nowait(date_time)

        print(f'{self.to_download.qsize()} items detected for downloading. Starting threads now...')

        for downloader in self.downloaders: downloader.start()
        for extractor in self.extractors: extractor.start()
        self.compressor.start()

        self.watch_to_mark_finished()


    def watch_to_mark_finished(self):
        while(True):
            try:
                file_date_time = self.to_mark_finished.get(block=True, timeout=5)
                print(f'Finished {file_date_time}, marking as done! {self.to_download.qsize()} items left in start queue.')
                self.date_time_dict[file_date_time] = True
                SaveFile.save('./save.json', self.date_time_dict)
            except Empty:
                pass


    def __del__(self):

        for downloader in self.downloaders: downloader.stop()
        for extractor in self.extractors: extractor.stop()
        self.compressor.stop()


        
AWS = AWSDownloader()