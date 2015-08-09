import bloop
import boto3

_PROFILE_NAME = "dvox-integ"
_SESSION = boto3.session.Session(profile_name=_PROFILE_NAME)
update = engine = bloop.Engine(
    session=_SESSION, atomic=False, consistent=False, save='update')
model = engine.model
overwrite = engine.context(save='overwrite')
atomic = engine.context(atomic=True)
consistent = engine.context(consistent=True)
atomic_overwrite = engine.context(save='overwrite', atomic=True)
