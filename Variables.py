import Transaction

class Variable:
    def __init__(self, vid):
        self.vid = 'x' + str(vid)
        self.isodd = vid % 2
        self.status = 0  # active = 0, recovering = 1
        self.commitvalues = [(0, vid * 10)]
        self.uncommit = None

    def is_odd(self):
        return self.isodd

    def recover(self):
        self.status = 1

    def activate(self):
        self.status = 0

    def read_commitvalue(self):
        return self.commitvalues[-1][1]

    def read_commited(self, transaction):
        if transaction.is_readonly():
            for ts, val in self.commitvalues:
                if ts <= transaction.get_timestamp():
                    res = val
                else:
                    break
            return res
        else:
            return self.commitvalues[-1][1]

    def read_uncommited(self, transaction):
        if self.uncommit and self.uncommit[0] == transaction.get_id():
            return self.uncommit[1]
        else:
            return self.read_commited(transaction)


    def write(self, transaction, val):
        self.uncommit = (transaction.get_id(), val)

    def commit(self, timestamp):
        if self.status == 1 :
            self.status = 0
        self.commitvalues.append((timestamp, self.uncommit[1]))
