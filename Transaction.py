class Transaction:
    def __init__(self, tid, timestamp, readonly, status):
        self.tid = tid
        self.timestamp = timestamp
        self.readonly = readonly #readonly = 1, readwrite = 0
        self.status = status # active = 0, waitting = 1, aborted = 2

    def get_id(self):
        return self.tid

    def get_timestamp(self):
        return self.timestamp

    def is_readonly(self):
        return self.readonly == 1

    def is_waitting(self):
        return self.status == 1

    def is_aborted(self):
        return self.status == 2

    def is_active(self):
        return self.status == 0

    def activate(self):
        if self.status != 0:
            self.status = 0

    def wait(self):
        if self.status != 1:
            self.status = 1

    def abort(self):
        self.status = 2

