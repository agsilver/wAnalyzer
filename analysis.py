import sqlite3
from tinydb import TinyDB, Query

# SQL
conn = sqlite3.connect("chat_data.db")
c = conn.cursor()
# JSON
jdb = TinyDB('analysis_db.json')
qu = Query()


def getppl():
    people = c.execute("SELECT DISTINCT author from data WHERE author != '' ").fetchall()
    pplist = [i[0] for i in people]
    return pplist


def topChar(char):  # input a char and find who uses it the most and how much
    ppl = getppl()
    scores = [['', 0] for i in range(len(ppl))]
    j = 0
    for person in ppl:
        scores[j][0] = person
        query = c.execute("SELECT content FROM data "
                          "WHERE author = ? AND content LIKE '%" + char + "%' ", (person,)).fetchall()
        for msg in query:
            scores[j][1] += msg[0].count(char)
        j += 1
    return scores
    # j = 0
    # for msg in msgs_list:
    #     occur = msg.count(char)
    #     try:
    #         scores[ppl.index(ppl_list[j])] += occur
    #     except ValueError:
    #         pass
    #     j += 1
    # score = max(scores)
    # top = ppl[scores.index(score)]


pplz = topChar('!')
print(pplz)


def personalStas():
    #  get list of all participants
    ppl = getppl()
    stats_list = []
    for person in ppl:
        word_count = 0
        letter_count = 0
        media = 0
        #  get all messages from author
        msgs = c.execute("SELECT content FROM data WHERE author = ?", (person,)).fetchall()
        #  get personal word & letter count
        for msg in msgs:
            if "<Media omitted>" not in msg[0]:
                word_count += msg[0].count(' ') + 1
                letter_count += len(msg[0])
            else:
                media += 1
        msg_count = len(msgs)
        stats_list.append((person, msg_count, word_count, letter_count, media))
    return stats_list
