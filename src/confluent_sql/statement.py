import logging
from dataclasses import dataclass
from enum import Enum
from typing import Optional

from .exceptions import OperationalError, DatabaseError


logger = logging.getLogger(__name__)


class Op(Enum):
    INSERT = 0
    UPDATE_BEFORE = 1
    UPDATE_AFTER = 2
    DELETE = 3

    def __str__(self):
        if self is self.INSERT:
            return "+I"
        elif self is self.UPDATE_BEFORE:
            return "-U"
        elif self is self.UPDATE_AFTER:
            return "+U"
        elif self is self.DELETE:
            return "-D"
        else:
            raise ValueError(
                f"Unknown value for Op: '{self.value}'. This is probably a bug"
            )


class Phase(Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    DELETING = "DELETING"
    FAILED = "FAILED"
    # This is not documented in the rest api docs, but mentioned here:
    # https://docs.confluent.io/cloud/current/flink/concepts/statements.html#flink-sql-statements
    DEGRADED = "DEGRADED"
    STOPPING = "STOPPING"
    STOPPED = "STOPPED"

    # This is only used internally,
    # never returned by the api.
    DELETED = "DELETED"


@dataclass
class Statement:
    statement_id: str
    name: str
    spec: dict
    status: dict
    traits: dict
    # Internal state
    _phase: Phase
    _deleted: bool = False

    @property
    def is_ready(self) -> bool:
        if self.is_bounded:
            return self.phase in [Phase.COMPLETED, Phase.STOPPED]
        else:
            return self.phase in [Phase.COMPLETED, Phase.STOPPED, Phase.RUNNING]

    @property
    def is_running(self) -> bool:
        return self.phase == Phase.RUNNING

    @property
    def is_completed(self) -> bool:
        return self.phase is Phase.COMPLETED

    @property
    def is_failed(self) -> bool:
        return self.phase is Phase.FAILED

    @property
    def is_degraded(self) -> bool:
        return self.phase is Phase.DEGRADED

    @property
    def phase(self) -> Phase:
        if self._deleted:
            return Phase.DELETED
        return self._phase

    @property
    def compute_pool_id(self) -> str:
        return self.spec["compute_pool_id"]

    @property
    def principal(self) -> str:
        return self.spec["principal"]

    @property
    def sql_kind(self) -> str:
        return self.traits["sql_kind"]

    @property
    def is_bounded(self) -> bool:
        return self.traits["is_bounded"]

    @property
    def is_append_only(self) -> bool:
        return self.traits["is_append_only"]

    @property
    def schema(self) -> Optional[dict]:
        return self.traits.get("schema", {}).get("columns")

    @property
    def connection_refs(self) -> list:
        return self.traits.get("connection_refs", [])

    @property
    def description(self) -> Optional[list[tuple]]:
        # This is required by the cursor object, see https://peps.python.org/pep-0249/#description
        # It's a list of 7-item tuples, the items represent:
        # (name, type_code, display_size, internal_size, precision, scale, null_ok)
        # TODO: we can probably add more info here.
        if self.schema is not None:
            return [
                (col["name"], col["type"]["type"], None, None, None, None, None)
                for col in self.schema
            ]
        return None

    @property
    def is_deleted(self) -> bool:
        """Has this statement been explicitly deleted?"""
        return self._deleted

    def set_deleted(self):
        """Mark this statement as deleted."""
        self._deleted = True

    @classmethod
    def from_response(cls, response: dict) -> "Statement":
        try:
            # Mandatory fields
            statement_id = response["metadata"]["uid"]
            name = response["name"]
            spec = response["spec"]
            status = response["status"]

            # Check the phase first.
            try:
                phase = Phase(status["phase"])
            except ValueError:
                raise OperationalError(
                    f"Received an unknown phase for statement from the server: {status['phase']}. "
                    "This is probably a bug"
                )

            # If it's failed, we won't get 'traits', and it's probably good to raise an error.
            # TODO: Should we instead set the phase and avoid erroring out here?
            if phase is Phase.FAILED:
                raise DatabaseError(status["detail"])

            traits = status["traits"]
        except KeyError as e:
            raise OperationalError(
                f"Error parsing statement response, missing {e}."
            ) from e

        return cls(statement_id, name, spec, status, traits, phase)
