import inspect
import re
import typing
from dataclasses import dataclass, field
from pprint import pprint

import sqlalchemy as sa
import yaml
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Query, Session, sessionmaker

with open("config.yaml", "r") as fh:
    YAML_CONNECTIONS = yaml.load(fh, Loader=yaml.FullLoader)

with open("secrets.yaml", "r") as fh:
    YAML_CONNECTIONS_SECRETS = yaml.load(fh, Loader=yaml.FullLoader)


@dataclass()
class Source:
    "Data source, considered one database"
    # Class variables
    sources = {}
    templates = YAML_CONNECTIONS["connections"]["templates"]

    name: str = None

    dict_yaml: typing.Dict[str, str] = field(default_factory=dict)
    # server: str = None
    # database: str = None
    # uid: str = None
    # pwd: str = None
    # driver: str = None
    # port: str = None

    reflect: typing.Dict = field(default_factory=dict)
    # template: str = None

    engine: sa.engine.Engine = None
    metadata: sa.MetaData = None
    session: Session = None

    @classmethod
    def from_name(cls, name: str):
        # print(YAML_CONNECTIONS["sources"][name])
        return cls(
            name=name,
            dict_yaml={
                **YAML_CONNECTIONS["connections"]["sources"][name].copy(),
                **YAML_CONNECTIONS_SECRETS["connections"][name].copy(),
            },
        )

    @classmethod
    def create_all(cls):
        return [
            cls.from_name(name=name)
            for name in YAML_CONNECTIONS["connections"]["sources"].keys()
        ]

    def __post_init__(self):
        # Add to sources
        self.__class__.sources[
            (
                self.name
                if self.name
                else f"{self.connection_dict['server']}/{self.connection_dict['database']}"
            )
        ] = self

        # Test the connection ->
        self.engine = sa.create_engine(self.connection_url)
        self.metadata = sa.MetaData()

        for k, v in self.reflect.items():
            self.metadata.reflect(bind=self.engine, schema=k, only=v)

        self.Base = automap_base(metadata=self.metadata)
        self.Base.prepare(engine=self.engine)

        self.Session = sessionmaker(bind=self.engine)

        # create a Session
        self.session = self.Session()

        # pprint(list(self.Base.classes))

    def __repr__(self):
        return f"<{self.__class__.__name__}({self.connection_dict['server']}/{self.connection_dict['database']})>"

    @property
    def template(self) -> str:
        return self.__class__.templates[self.dict_yaml["template"]]

    @property
    def template_variables(self) -> typing.List[str]:
        return list(
            re.search(
                r"(?<!{){(?!{)((?:\w)+)(?<!})}(?!})", self.connection_url
            ).groups()
        )

    @property
    def connection_dict(self) -> dict:
        return {
            k: v
            for k, v in self.dict_yaml.items()
            if k in ["server", "database", "uid", "pwd", "driver", "port"]
        }

    @property
    def connection_url(self) -> str:
        return self.template.format(
            **self.dict_yaml
            # **{k: v for k, v in self.dict_yaml.items() if k in self.template_variables}
        )

    @property
    def query(self) -> Query:
        return self.session.query

    @property
    def c(self):
        "Quick accessor to classes"
        return self.Base.classes

    @property
    def execute(self):
        return self.engine.connect().execute

    @property
    def connection(self) -> sa.engine.Connection:
        return self.engine.connect()


if __name__ == "__main__":

    s = Source.from_name("MAIN")

    pprint(s.dict_yaml)
