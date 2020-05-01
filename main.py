from uuid import uuid4
from parse import *
from tinydb import TinyDB, Query

# SQL
conn = sqlite3.connect("chat_data.db")
c = conn.cursor()
# JSON
jdb = TinyDB('analysis_db.json')
qu = Query()


def newChat():
    chat_name = input("Enter the chat file\'s name to analyze: ")
    chat_path = r".\chats\\" + chat_name + ".txt"
    request_id = str(uuid4())
    getData(chat_path, request_id)


if __name__ == '__main__':
    newChat()
