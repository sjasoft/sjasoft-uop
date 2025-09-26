class NoSuchObject(Exception):
    def __init__(self, uid):
        super().__init__('no object with uuid %s' % uid)

