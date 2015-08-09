class CreateFailed(Exception):
    ''' Failed to create (and persist) an object after n retries '''
    pass


class InUse(Exception):
    ''' Thrown when a resource (lock, etc) is already in use '''
    pass


class RetryOperation(Exception):
    '''
    Thrown when an operation failed, but can be retried without modification
    '''
    pass


class Expired(Exception):
    ''' Thrown when a lock is no longer valid '''
    pass
