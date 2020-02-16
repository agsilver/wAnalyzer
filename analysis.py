import sqlite3
import statistics

conn = sqlite3.connect("chat_data.db")
c = conn.cursor()


def tpl2list(sql_tuples, idx):
    lst = []
    [lst.append(tpl[idx]) for tpl in sql_tuples]
    return lst


def getppl():
    ppll = []
    pplt = c.execute("SELECT DISTINCT author from data")
    for author in pplt:
        if author[0] != '':
            ppll.append(author[0])
    return ppll


def top_char_user(char):
    # input a char and find who uses it the most and how much
    ppl = getppl()
    scores = [0] * len(ppl)
    cont_msgs = c.execute("SELECT content, author FROM data "
                          "WHERE content LIKE '%" + char + "%' ").fetchall()  # find all messages containing a character
    ppl_list = tpl2list(cont_msgs, 1)
    msgs_list = tpl2list(cont_msgs, 0)
    j = 0
    for msg in msgs_list:
        occur = msg.count(char)
        try:
            scores[ppl.index(ppl_list[j])] += occur
        except ValueError:
            pass
        j += 1
    score = max(scores)
    top = ppl[scores.index(score)]
    return top, score


keyword = ""
winner, points = top_char_user(keyword)
print("The user who uses '" + keyword + "' the most is: " + winner + "\nTimes used: " + str(points) + ".")

