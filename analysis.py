import sqlite3
import statistics

conn = sqlite3.connect("chat_data.db")
c = conn.cursor()


def tpl2list(sql_tuples, idx):
    lst = []
    [lst.append(tpl[idx]) for tpl in sql_tuples]
    return lst


def top_char_user(char):
    # input a char and find who uses it the most and how much
    occur = []
    cont_msgs = c.execute("SELECT content, author FROM data "
                          "WHERE content LIKE '%" + char + "%' ")  # find all messages containing a character
    ppl_list = tpl2list(cont_msgs, 1)
    msgs_list = tpl2list(cont_msgs, 2)
    [occur.append(msg.count(char)) for msg in msgs_list]
    winner = statistics.mode(ppl_list)
    return winner


winnner = top_char_user('!')
print(winnner)
