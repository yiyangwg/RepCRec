"""
Created on Wed Nov 29 16:10:02 2017

Author: Yiyang Wang, Yulu Qian

"""

class Operation:
    def __init__(self, type, tid, vid=None, val=None):
        self.type = type
        self.tid = tid
        self.vid = vid
        self.val = val

    def get_type(self):
        return self.type

    def get_tid(self):
        return self.tid

    def get_vid(self):
        return self.vid

    def get_val(self):
        return self.val
