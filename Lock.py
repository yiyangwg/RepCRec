"""
Created on Wed Nov 29 16:10:02 2017

Author: Yiyang Wang, Yulu Qian

"""

from Transaction import Transaction
from Variables import Variable

class Lock:
    def __init__(self, _variables):
        self.RL_table = {}
        self.WL_table = {}
        self.variables = _variables
        self.release_all_locks()


    def release_all_locks(self):
        for v in self.variables:
            self.RL_table[v] = []
            self.WL_table[v] = None

    def hold_read_lock(self, transaction, vid):
        if vid in self.RL_table:
            transactions = self.RL_table[vid]
            if transaction.get_id() in transactions:
                return True
        return False

    def hold_write_lock(self, transaction, vid):
        if vid in self.WL_table:
            if self.WL_table[vid] == transaction.get_id():
                return True
        return False


    def acquire_read_lock(self, transaction, vid):
        if not self.hold_read_lock(transaction, vid):
            if self.WL_table[vid] != None and self.WL_table[vid] != transaction.get_id():
                return {'wait':[self.WL_table[vid]]}
            self.RL_table[vid].append(transaction.get_id())
            return

    def acquire_write_lock(self, transaction, vid):
        if not self.hold_write_lock(transaction, vid):
            if self.WL_table[vid] != None and self.WL_table[vid] != transaction.get_id():
                return {'wait': [self.WL_table[vid]]}
            if self.RL_table[vid] != []:
                if not (len(self.RL_table[vid]) == 1 and self.RL_table[vid][0] == transaction.get_id()):
                    return {'wait': self.RL_table[vid]}
            self.WL_table[vid] = transaction.get_id();
            return

    def release_read_lock(self, transaction):
        for vid in self.RL_table.keys():
            if (transaction.get_id() in self.RL_table[vid]):
                self.RL_table[vid].remove(transaction.get_id())

    def release_write_lock(self, transaction):
        for vid in self.WL_table.keys():
            if (self.WL_table[vid] == transaction.get_id()):
                self.WL_table[vid] = None
