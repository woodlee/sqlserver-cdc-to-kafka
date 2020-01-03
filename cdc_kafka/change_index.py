from functools import total_ordering

from . import constants


@total_ordering
class ChangeIndex(object):
    # SQL Server CDC capture tables have a single compound index on (lsn, seqval, operation)
    def __init__(self, lsn: bytes, seqval: bytes, operation: int):
        self.lsn: bytes = lsn
        self.seqval: bytes = seqval
        if isinstance(operation, int):
            self.operation: int = operation
        else:
            self.operation: int = constants.CDC_OPERATION_NAME_TO_ID[operation]

    def __eq__(self, other: 'ChangeIndex') -> bool:
        return self.lsn + self.seqval + bytes([self.operation]) == \
               other.lsn + other.seqval + bytes([other.operation])

    def __lt__(self, other: 'ChangeIndex') -> bool:
        return self.lsn + self.seqval + bytes([self.operation]) < \
               other.lsn + other.seqval + bytes([other.operation])

    def __repr__(self) -> str:
        return f'0x{self.lsn.hex()}:0x{self.seqval.hex()}:{self.operation}'


BEGINNING_CHANGE_INDEX = ChangeIndex(b'\x00' * 10, b'\x00' * 10, 0)
