import sqlite3

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
