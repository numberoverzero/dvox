from bloop import String


class Position(String):
    """ stores [2, 3, 4] as '2:3:4' """
    def dynamo_load(self, value):
        values = value.split(":")
        return list(map(int, values))

    def dynamo_dump(self, value):
        return ":".join(map(str, value))


class StringEnum(String):
    """Store an enum by the names of its values"""
    def __init__(self, enum):
        self.enum = enum
        super().__init__()

    def dynamo_load(self, value):
        value = super().dynamo_load(value)
        return self.enum[value]

    def dynamo_dump(self, value):
        value = value.name
        return super().dynamo_dump(value)
