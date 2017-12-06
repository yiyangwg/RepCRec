"""
Created on Wed Nov 29 16:10:02 2017

Author: Yiyang Wang, Yulu Qian

"""

from Variables import Variable
from Transaction import Transaction
from Lock import Lock
from Site import Site
from Operation import Operation

class TransactionManager:
    def __init__(self):
        self.sites = {}
        for i in range(1, 11):
            self.sites[i] = Site(i)

        self.transactions = {}
        self.waitlist = []
        self.t_sites = {}
        self.wg = {}
        self.cycle = []

    def getyoungest(self):
        ts = -1
        for tid in self.cycle:
            if ts < self.transactions[tid].get_timestamp():
                ts = self.transactions[tid].get_timestamp()
                id = tid
        return id

    def checkDeadlock(self, tid, ret, op):
        waitfor_tids = ret['wait']
        if tid in  waitfor_tids:
            waitfor_tids.remove(tid)

        for id in waitfor_tids:
            if not id in self.wg[tid]:
                self.wg[tid].append(id)

        self.cycle = []
        if self.isDeadlocked('start', tid):
            print("Deadlock detected!")

            tid = self.getyoungest()
            self.abort(tid)
        else:
            self.add_waitlist(op)

    def isDeadlocked(self, tid, start):
        if tid == start:
            return True
        if tid == 'start':
            tid = start
        self.cycle.append(tid)

        for next in self.wg[tid]:
            if self.isDeadlocked(next, start):
                return True

        self.cycle.remove(tid)
        return False


    def beginRO(self, tid, timestamp):
        if tid not in self.transactions:
            transaction = Transaction(tid, timestamp, 1, 0)
            self.transactions[tid] = transaction
            self.t_sites[tid] = []
            self.wg[tid] = []
            print('Read Only', tid, 'begin at', timestamp)
        else:
            print(tid, 'already exists')

    def begin(self, tid, timestamp):
        if tid not in self.transactions:
            transaction = Transaction(tid, timestamp, 0, 0)
            self.transactions[tid] = transaction
            self.t_sites[tid] = []
            self.wg[tid] = []
            print('Read Write', tid, 'begin at', timestamp)
        else:
            print(tid, 'already exists')

    def read(self, tid, vid):
        if self.transactions[tid].is_aborted():
            print(tid, 'is aborted')
            return
        sites = self.assignsite(int(vid[1:]))
        for sid in sites:
            if self.sites[sid].is_active():
                self.add_tsites(tid, sid, vid)
                ret = self.sites[sid].read(self.transactions[tid], vid)
                if ret == None:
                    self.transactions[tid].activate()
                    return
                else:
                    self.checkDeadlock(tid, ret, Operation('read', tid, vid))
                    return
        print('No site is available to read', vid, 'in', tid)
        self.add_waitlist(Operation('read', tid, vid))


    def write(self, tid, vid, val):
        if self.transactions[tid].is_aborted():
            print(tid, 'is aborted')
            return
        sites = self.assignsite(int(vid[1:]))
        success = 0
        for sid in sites:
            if self.sites[sid].is_active():
                self.add_tsites(tid, sid, vid)
                ret = self.sites[sid].write(self.transactions[tid], vid, val)
                if ret == None:
                    success += 1
                else:
                    self.checkDeadlock(tid, ret, Operation('write', tid, vid, val))
                    return
        if success > 0:
            self.transactions[tid].activate()
            print('Write', vid, '=', val, 'in', tid)
            return
        print('No site is available to write', vid, 'in', tid)
        self.add_waitlist(Operation('write', tid, vid, val))

    def recover(self, sid):
        self.sites[sid].recover()
        print('Site', sid, 'recovered')
        self.retry_waiting()

    def fail(self, sid):
        self.sites[sid].fail()
        print('Site', sid, 'failed')
        for tid in self.t_sites.keys():
            if sid in self.t_sites[tid]:
                if not self.transactions[tid].is_aborted():
                    self.abort(tid)


    def clean_wg(self, tid):
        for id in self.wg.keys():
            if tid in self.wg[id]:
                self.wg[id].remove(tid)

    def end(self, tid, timestamp):
        if self.transactions[tid].is_aborted():
            print(tid, 'is aborted')
            return
        if not self.transactions[tid].is_readonly():
            for sid in self.t_sites[tid]:
                if self.sites[sid].is_active():
                    self.sites[sid].commit(self.transactions[tid], timestamp)
                else:
                    print('Site', sid, 'failed, aborting', tid);
                    self.abort(tid)
                    return
            self.retry_waiting()

            print(tid, 'ended')
            self.clean_wg(tid)
            del self.t_sites[tid]
            del self.transactions[tid]



    def assignsite(self, vid):
        if vid % 2 == 1:
            return [(vid % 10) + 1]
        else:
            return [x for x in range(1, 11)]

    def add_waitlist(self, op):
        if not self.transactions[op.get_tid()].is_waitting():
            print('Put', op.get_tid(), 'into wait list')
            self.waitlist.append(op)
            self.transactions[op.get_tid()].wait()


    def retry_waiting(self):
        i = 0
        while i < len(self.waitlist):
            op = self.waitlist[i]
            tid = op.get_tid()
            vid = op.get_vid()
            if op.get_type() == 'read':
                self.read(tid, vid)
            elif op.get_type() == 'write':
                self.write(tid, vid, op.get_val())
            if self.transactions[tid].is_active():
                self.waitlist.remove(op)
            else:
                i += 1


    def abort(self, tid):
        for sid in self.t_sites[tid]:
            self.sites[sid].abort(self.transactions[tid])
            self.transactions[tid].abort()
        print('Abort', tid)
        self.clean_wg(tid)
        self.retry_waiting()


    def add_tsites(self, tid, sid, vid):
        if not self.transactions[tid].is_readonly():
            if not sid in self.t_sites[tid]:
                self.t_sites[tid].append(sid)



    def dumpall(self):
        ret = {}
        for sid in self.sites.keys():
            ret[str(sid)] = self.sites[sid].dump()
            print(sid, ret[str(sid)])

    def dump_v(self, vid):
        sites = self.assignsite(int(vid[1:]))
        ret = {}
        for s in sites:
            ret[str(s)] = self.sites[s].dumpv(vid)
            print(s, ret[str(s)])

    def dump_s(self, sid):
        print(sid, self.sites[sid].dump())