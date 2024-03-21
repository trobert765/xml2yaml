from __future__ import annotations

from pydantic import BaseModel


class Input(BaseModel):
    delimiter: str
    format: str
    header: bool


class Inputs(BaseModel):
    input: Input


class Output(BaseModel):
    emptyValue: str
    format: str
    handler: str
    lineSep: str
    num_partitions: int
    overwrite: bool
    quoteAll: bool
    sep: str


class Outputs(BaseModel):
    output: Output


class Model(BaseModel):
    inputs: Inputs
    outputs: Outputs
