from uuid import uuid4
from parse import *


def main():
    chat_name = r".\chats\\" + input("Enter the chat file\'s name to analyze: ") + ".txt"
    request_id = str(uuid4())
    getData(chat_name, request_id)


if __name__ == '__main__':
    main()
