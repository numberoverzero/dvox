from bloop import String


class Position(String):
    """ stores [2, 3, 4] as '2:3:4' """
    def dynamo_load(self, value):
        values = value.split(":")
        return list(map(int, values))

    def dynamo_dump(self, value):
        return ":".join(map(str, value))
