from doc import Define
from openai import OpenAI
import requests
import json


def find_assistant_info(target_name:str):
    url = "https://api.openai.com/v1/assistants"

    headers = {
        "Authorization": f"Bearer {Define.OPENAI_KEY}",
        "Content-Type": "application/json",
        "OpenAI-Beta": "assistants=v2"
    }
    
    params = {
        "limit": 100   
    }
    
    response = requests.get(url, headers=headers, params=params)
    if response.status_code == 200:
        projects = response.json()
        for obj in projects["data"]:
            if obj["name"] == target_name:
                return obj
    
    return {}

def create_thread():
    url = "https://api.openai.com/v1/threads"

    headers = {
        "Authorization": f"Bearer {Define.OPENAI_KEY}",
        "Content-Type": "application/json",
        "OpenAI-Beta": "assistants=v2"
    }

    data = {
        "messages": [],  # Optional: 대화 시작 시 포함할 메시지 리스트
        "tool_resources": {},  # Optional: Assistant가 사용할 리소스 설정
        "metadata": {}  # Optional: 객체에 부착할 메타데이터 설정
    }

    response = requests.post(url, headers=headers, data=json.dumps(data))
    if response.status_code == 200:
        return response.json()
    
def create_message(thread_id:str):
    url = f"https://api.openai.com/v1/threads/{thread_id}/messages"

    headers = {
        "Authorization": f"Bearer {Define.OPENAI_KEY}",
        "Content-Type": "application/json",
        "OpenAI-Beta": "assistants=v2"
    }

    data = {
        "role": "user",
        "content": "2024-08-29 KR",
    }

    response = requests.post(url, headers=headers, data=json.dumps(data))
    if response.status_code == 200:
        return response.json()
    


assistant = find_assistant_info("Stock Man")
thread = create_thread()
request_msg = create_message(thread["id"])

print(request_msg)
