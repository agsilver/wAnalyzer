import sqlite3
from sys import stdout
from timeit import default_timer as timer
import re
import uuid


current_chat = r'.\chats\sherks.txt'
conn = sqlite3.connect("chat_data.db")
c = conn.cursor()


#  Database tools
class Database:
    @staticmethod  # dump entire message data to the table.
    def dump(date_sent, time_sent, author_name, msg_content, uid):
        c.execute("INSERT INTO data (date, time, author, content, id) values (?, ?, ?, ?, ?)",
                  (date_sent, time_sent, author_name, msg_content, uid))

    @staticmethod  # insert data to one column only.
    def insert(column, text):
        c.execute("INSERT INTO data (%s) values (?)" % (column,), (text,))

    @staticmethod  # clear the entire table.
    def clearTable():
        c.execute("DELETE from data;")

    @staticmethod  # add text to data column
    def updateRow(msg_content, uid):
        c.execute('UPDATE data set content = content || ? WHERE id = ?',  (msg_content, uid))


#  Open a chat file and an error log file and make sure the encoding is correct.
def openChat(path):
    chat_file = open(path, 'r', encoding='utf-8')
    log_file = open('errors_log.txt', 'w', encoding='utf-8')
    return chat_file, log_file


# number of lines in chat file.
def chatLength(path):
    chat_file = open(path, 'r', encoding='utf-8')
    k = len(list(chat_file)) + 1
    return k


#  progress display
def progress(part, whole, cmplt_msg):
    percent_raw = round((part/whole)*100, 1)
    percent = str(percent_raw) + "%"
    stdout.write('\r')
    if percent_raw == 100.0:
        stdout.write("100.0% - " + cmplt_msg)
    else:
        stdout.write(percent)
    stdout.flush()


#  Parse chat rows for data.
def parseChat(row):
    # get time and date
    date_sent, time_sent, author_name, msg_content = ['']*4
    unique_id = str(uuid.uuid4())
    # RegEx for WhatsApp date formats.
    find_date = re.search(r'([1-9]|1[0-2])(/)([1-9]|[1-3][0-9])(/)\d{2}(, )\d', row)
    if find_date is None and row != '\n':  # continued message
        msg_content = row
        date_sent, time_sent, author_name = [None] * 3
    elif find_date is None and row == '\n':  # empty row
        pass
    else:  # regular row
        a, z = find_date.span()  # get date start and end
        date_sent = row[a:z-3]  # get date
        time_sent = row[z-1:z+4]  # get time
        # get author's name
        i = z + 7
        if ":" in row[i:]:
            while row[i] != ":":
                author_name += row[i]
                i += 1
        # get message content
        msg_content = row[i+1:]
    #  check if row is empty, return nothing
    if msg_content == '':
        return [None]*5
    #  if everything is standard
    else:
        return date_sent, time_sent, author_name, msg_content, unique_id


def getData():  # ectract raw data from chat and dump in db
    chat, log = openChat(current_chat)
    tot_lines = chatLength(current_chat)
    prog = 0
    Database.clearTable()
    id_holder = ''
    batch = 1
    batch_size = 500
    cmplt_msg = 'chat data extraction completed.'
    for line in chat:
        try:
            date, time, author, content, uid = parseChat(line)
            if content is None:
                pass
            elif date is None:
                Database.updateRow(content, id_holder)
            else:
                Database.dump(date, time, author, content, uid)
                id_holder = uid
        except IndexError:
            log.write("Index error: \n" + line)
        prog += 1
        try:
            if prog % batch_size == 0:
                conn.commit()
                batch += 1
        except ValueError:
            print('Error in batch #', batch)
        progress(prog, tot_lines, cmplt_msg)
    conn.commit()
    log.close()
    conn.close()


# get all group participants
def get_people():
    people = c.execute('select distinct author from data')
    names = []
    for name in people:
        if name[0] != '':
            names.append(name[0])
    return tuple(names)


def main():
    start = timer()
    getData()
    end = timer()
    tot_time = end - start
    print('\nTime elapsed: ' + str(round(tot_time / 60, 0)) + ' min.')


if __name__ == "__main__":
    main()
