from functools import total_ordering
from typing import Dict, Any

from . import constants


@total_ordering
class ChangeIndex(object):
    def __init__(self, lsn: bytes, seqval: bytes, operation: int) -> None:
        self.lsn: bytes = lsn
        self.seqval: bytes = seqval
        if isinstance(operation, int):
            self.operation: int = operation
        elif isinstance(operation, str):
            self.operation: int = constants.CDC_OPERATION_NAME_TO_ID[operation]
        else:
            raise Exception(f'Unrecognized type for parameter `operation` (type: {type(operation)}, '
                            f'value: {operation}).')

    def __eq__(self, other: 'ChangeIndex') -> bool:
        if isinstance(other, ChangeIndex):
            return self.lsn + self.seqval + bytes([self.operation]) == \
                   other.lsn + other.seqval + bytes([other.operation])
        return False

    def __lt__(self, other: 'ChangeIndex') -> bool:
        return self.lsn + self.seqval + bytes([self.operation]) < \
               other.lsn + other.seqval + bytes([other.operation])

    # For user-friendly display in logging etc.; not the format to be used for persistent data storage
    def __repr__(self) -> str:
        lsn = self.lsn.hex()
        seqval = self.seqval.hex()
        return f'0x{lsn[:8]} {lsn[8:16]} {lsn[16:]}:0x{seqval[:8]} {seqval[8:16]} {seqval[16:]}:{self.operation}'

    # Converts from binary LSN/seqval to a string representation that is more friendly to some things that may
    # consume this data. The stringified form is also "SQL query ready" for pasting into SQL Server queries.
    def to_avro_ready_dict(self) -> Dict[str, str]:
        return {
            constants.LSN_NAME: f'0x{self.lsn.hex()}',
            constants.SEQVAL_NAME: f'0x{self.seqval.hex()}',
            constants.OPERATION_NAME: constants.CDC_OPERATION_ID_TO_NAME[self.operation]
        }

    @staticmethod
    def from_avro_ready_dict(avro_dict: Dict[str, Any]) -> 'ChangeIndex':
        return ChangeIndex(
            int(avro_dict[constants.LSN_NAME][2:], 16).to_bytes(10, "big"),
            int(avro_dict[constants.SEQVAL_NAME][2:], 16).to_bytes(10, "big"),
            constants.CDC_OPERATION_NAME_TO_ID[avro_dict[constants.OPERATION_NAME]]
        )


LOWEST_CHANGE_INDEX = ChangeIndex(b'\x00' * 10, b'\x00' * 10, 0)
HIGHEST_CHANGE_INDEX = ChangeIndex(b'\xff' * 10, b'\xff' * 10, 4)
