import sqlite3
from sys import stdout

current_chat = r'C:\Users\eyalv\PycharmProjects\wAnalyzer\chats\sherks.txt'
errorlog_path = r'C:\Users\eyalv\PycharmProjects\wAnalyzer\log.txt'
conn = sqlite3.connect("sherks.db")
c = conn.cursor()


#  Database tools
class Database:
    @staticmethod  # dump data to the table
    def dump(date_sent, time_sent, author_name, msg_content):
        c.execute("INSERT INTO raw (date, time, author, content) values (?, ?, ?, ?)",
                  (date_sent, time_sent, author_name, msg_content))
        conn.commit()

    @staticmethod  # clear the entire table
    def clearTable():
        c.execute("DELETE from raw;")


#  Open a chat file and an error log file and make sure the encoding is correct.
def openChat(path, log_path):
    file = open(path, 'r', encoding='utf-8')
    log_file = open(log_path, 'w', encoding='utf-8')
    return file, log_file


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
    i = 0
    if row[0].isdigit() is True and row[1].isdigit() is True and row[2] == '/':
        if row[8] == ",":  # 4-digits day and month
            date_sent = row[0:8]
            time_sent = row[10:15]
            i = 18
        elif row[7] == ",":  # 3-digits day month
            date_sent = row[0:7]
            time_sent = row[9:14]
            i = 17
        elif row[6] == ",":  # 2-digits day and month
            date_sent = row[0:6]
            time_sent = row[8:13]
            i = 16
        # get author's name
        if ":" in row[i:]:
            while row[i] != ":":
                author_name += row[i]
                i += 1
        # get message content
        msg_content = row[i+1:]
    elif row == '\n':
        pass
    else:
        msg_content = row
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
    chat, log = openChat(current_chat, errorlog_path)
    tot_lines = chatLength(chat)
    prog = 0
    chat, log = openChat(current_chat, errorlog_path)
    Database.clearTable()
    for line in chat:
        try:
            date, time, author, content = parseChat(line)
            Database.dump(date, time, author, content)
        except IndexError:
            log.write("Error: \n" + line)
        prog += 1
        progress(prog, tot_lines)
    log.close()
    conn.close()


if __name__ == "__main__":
    main()
