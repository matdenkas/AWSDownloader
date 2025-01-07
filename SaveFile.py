
import json


def save(file_path: str, data: dict[any]) -> None:
    json_txt = json.dumps(data)

    with open(file_path, 'w') as sf:
        sf.write(json_txt)

def load(file_path: str) -> dict[any]:
    with open(file_path, 'r') as sf:
        json_txt = sf.read()
    
    return json.loads(json_txt)