"""
Created on Wed Nov 29 16:10:02 2017

Author: Yiyang Wang, Yulu Qian

"""

from TransactionManager import TransactionManager

TM = TransactionManager()
filename = "input.txt"
lines = open(filename, 'r').readlines()

for i in range(1, len(lines) + 1):
    line = lines[i - 1]
    if 'beginRO' in line:
        st = line.split('(')
        tid = st[1].split(')')[0]
        TM.beginRO(tid, i)
        continue

    if 'begin' in line:
        st = line.split('(')
        tid = st[1].split(')')[0]
        TM.begin(tid, i)
        continue

    if 'R(' in line:
        st = line.split('(')[1]
        st = st.split(')')[0]
        st = st.split(',')
        tid = st[0]
        vid = st[1]
        TM.read(tid, vid)
        continue

    if 'W(' in line:
        st = line.split('(')[1]
        st = st.split(')')[0]
        st = st.split(',')
        tid = st[0]
        vid = st[1]
        val = st[2]
        TM.write(tid, vid, int(val))
        continue

    if 'end' in line:
        st = line.split('(')
        tid = st[1].split(')')[0]
        TM.end(tid, i)
        continue

    if 'dump()' in line:
        TM.dumpall()
        continue

    if 'dump(x' in line:
        st = line.split('(')[1]
        vid = st.split(')')[0]
        TM.dump_v(vid)
        continue

    if 'dump' in line:
        st = line.split('(')[1]
        sid = st.split(')')[0]
        TM.dump_s(int(sid))

    if 'fail' in line:
        st = line.split('(')
        sid = st[1].split(')')[0]
        TM.fail(int(sid))
        continue

    if 'recover' in line:
        st = line.split('(')
        sid = st[1].split(')')[0]
        TM.recover(int(sid))
        continue

