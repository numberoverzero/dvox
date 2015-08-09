import arrow
import uuid
from bloop import Column, DateTime, UUID, String, ConstraintViolation
from dvox import engines
from dvox.app import config
from dvox.exceptions import CreateFailed


class Worker(engines.model):
    id = Column(UUID, hash_key=True)
    ip = Column(String)
    expires = Column(DateTime, name='e')

    @classmethod
    def unique(cls, ip):
        retries = config["CREATE_RETRIES"]
        while retries:
            expires = arrow.now().replace(
                seconds=config["WORKER_TIMEOUT_SECONDS"])
            obj = cls(id=uuid.uuid4(), ip=ip, expires=expires)
            try:
                engines.atomic.save(obj)
            except ConstraintViolation:
                retries -= 1
            else:
                return obj
        raise CreateFailed

    def renew(self):
        self.expires = arrow.now().replace(
            seconds=config["WORKER_TIMEOUT_SECONDS"])
        engines.update.save(self)
