import bloop
import boto3.session

_PROFILE_NAME = "dvox-integ"
_SESSION = boto3.session.Session(profile_name=_PROFILE_NAME)
engine = bloop.Engine(session=_SESSION)
atomic_engine = engine.context(atomic=True, consistent=True)

config = {
    "CREATE_RETRIES": 5,
    "CHUNK_LOCK_TIMEOUT_SECONDS": 30
}
