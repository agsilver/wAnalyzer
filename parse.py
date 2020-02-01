import sqlite3
from sys import stdout
from timeit import default_timer as timer
import re


current_chat = r'.\chats\sherks.txt'
conn = sqlite3.connect("sherks.db")
c = conn.cursor()


#  Database tools
class Database:
    @staticmethod  # dump entire message data to the table.
    def dump(date_sent, time_sent, author_name, msg_content):
        c.execute("INSERT INTO raw (date, time, author, content) values (?, ?, ?, ?)",
                  (date_sent, time_sent, author_name, msg_content))
        conn.commit()

    @staticmethod  # insert data to one column only.
    def insert(column, text):
        c.execute("INSERT INTO raw (%s) values (?)" % (column,), (text,))
        conn.commit()

    @staticmethod  # clear the entire table.
    def clearTable():
        c.execute("DELETE from raw;")


#  Open a chat file and an error log file and make sure the encoding is correct.
def openChat(path):
    chat_file = open(path, 'r', encoding='utf-8')
    log_file = open('errors_log.txt', 'w', encoding='utf-8')
    return chat_file, log_file


# number of lines in chat file.
def chatLength(f):
    temp_file = f
    k = 0
    for k, l in enumerate(temp_file):
        pass
    return k + 1


#  Parse chat rows for info.
def parseChat(row):
    # get time and date
    date_sent = ''
    time_sent = ''
    author_name = ''
    msg_content = ''
    find_date = re.search(r'([1-9]|1[0-2])(/)([1-9]|[1-3][0-9])(/)\d{2}', row)
    if find_date is None and row != '\n':  # continued message
        msg_content = row
    elif find_date is None and row == '\n':  # empty row
        pass
    else:  # regular row
        a, z = find_date.span()  # get date start and end
        date_sent = row[a:z]  # get date
        time_sent = row[z+2:z+7]  # get time
        # get author's name
        i = z + 10
        if ":" in row[i:]:
            while row[i] != ":":
                author_name += row[i]
                i += 1
        # get message content
        msg_content = row[i+1:]
    return date_sent, time_sent, author_name, msg_content


#  progress display
def progress(part, whole):
    percent_raw = round((part/whole)*100, 1)
    percent = str(percent_raw) + "%"
    stdout.write('\r')
    if percent_raw == 100.0:
        stdout.write("100.0% - parsing completed.")
    else:
        stdout.write(percent)
    stdout.flush()


def main():
    chat, log = openChat(current_chat)
    tot_lines = chatLength(chat)
    prog = 0
    chat, log = openChat(current_chat)
    Database.clearTable()
    start = timer()
    for line in chat:
        try:
            date, time, author, content = parseChat(line)
            Database.dump(date, time, author, content)
        except IndexError:
            log.write("Error: \n" + line)
        prog += 1
        progress(prog, tot_lines)
    end = timer()
    tot_time = end - start
    print('\nTime elapsed: ' + str(round(tot_time / 60, 0)) + ' min.')
    log.close()
    conn.close()


if __name__ == "__main__":
    main()
