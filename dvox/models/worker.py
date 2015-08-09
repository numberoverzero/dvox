import uuid
from bloop import Column, UUID, String, ConstraintViolation
from dvox.app import config
from dvox.engines import engine
from dvox.exceptions import CreateFailed


class Worker(engine.model):
    id = Column(UUID, hash_key=True)
    ip = Column(String)

    @classmethod
    def unique(cls, ip):
        retries = config["CREATE_RETRIES"]
        does_not_exist = cls.id.is_(None)
        while retries:
            obj = cls(id=uuid.uuid4(), ip=ip)
            try:
                engine.save(obj, condition=does_not_exist)
            except ConstraintViolation:
                retries -= 1
            else:
                return obj
        raise CreateFailed()
