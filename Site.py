from Transaction import Transaction
from Variables import Variable
from Lock import Lock

class Site:
    def __init__(self, id):
        self.sid = id
        self.variables = {}
        self.status = 0 # active = 0, failed = 1
        self.init_variables()
        self.locks = Lock(self.variables)


    def init_variables(self):
        for x in range(1, 21):
            vid = 'x' + str(x)
            if x % 2 == 1:
                if self.sid == x % 10 + 1:
                    self.variables[vid] = Variable(x)
            else:
                self.variables[vid] = Variable(x)

    def is_active(self):
        return self.status == 0

    def abort(self, transaction):
        self.locks.release_read_lock(transaction)
        self.locks.release_write_lock(transaction)

    def fail(self):
        if self.is_active():
            self.status = 1
            self.locks.release_all_locks()

    def read(self, transaction, vid):
        if self.variables[vid].status == 0:
            if self.locks.hold_read_lock(transaction, vid):
                print('Read', vid, '=', self.variables[vid].read_uncommited(transaction), 'at Site', self.sid, 'in', transaction.get_id())
                return
            else:
                ret = None
                if not transaction.is_readonly():
                    ret = self.locks.acquire_read_lock(transaction, vid)
                if ret != None:
                    return ret
                print('Read', vid, '=', self.variables[vid].read_commited(transaction), 'at Site', self.sid, 'in', transaction.get_id())
                return

    def write(self, transaction, vid, val):
        try:
            ret = None
            ret = self.locks.acquire_write_lock(transaction, vid)
            if ret != None:
                return ret
            self.variables[vid].write(transaction, val)
            return
        except WaitFor:
            return [WaitFor.args]

    def commit(self, transaction, timestamp):
        for vid in self.variables.keys():
            if self.locks.hold_write_lock(transaction, vid):
                self.variables[vid].commit(timestamp)
        self.locks.release_write_lock(transaction)
        self.locks.release_read_lock(transaction)

    def dump(self):
        ret = {}
        for vid in self.variables.keys():
            ret[vid] = self.variables[vid].read_commitvalue()
        return ret

    def dumpv(self, vid):
        ret = {}
        if vid in self.variables:
            ret[vid] = self.variables[vid].read_commitvalue()
        return ret

    def recover(self):
        if not self.is_active():
            for vid in self.variables.keys():
                if self.variables[vid].is_odd() == 0:
                    self.variables[vid].recover()
            self.status = 0