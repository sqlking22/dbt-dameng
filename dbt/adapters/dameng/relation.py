"""
Copyright (c) 2023, Dameng and/or its affiliates.
Copyright (c) 2022, Oracle and/or its affiliates.
Copyright (c) 2020, Vitor Avancini

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

     https://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
"""
from dataclasses import dataclass, field

from dbt.adapters.base.relation import BaseRelation, Policy


@dataclass
class DamengQuotePolicy(Policy):
    schema: bool = False
    identifier: bool = False


@dataclass
class DamengIncludePolicy(Policy):
    schema: bool = True
    identifier: bool = True


@dataclass(frozen=True, eq=False, repr=False)
class DamengRelation(BaseRelation):
    quote_policy: DamengQuotePolicy = field(default_factory=lambda: DamengQuotePolicy())
    include_policy: DamengIncludePolicy = field(default_factory=lambda: DamengIncludePolicy())

    @staticmethod
    def add_ephemeral_prefix(name):
        return f'dbt__cte__{name}__'


