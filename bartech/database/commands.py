import click
import datetime as dt

from alembic.config import Config
from alembic import command
from pathlib import Path

from .sources import Source


source = Source.from_name("MAIN")

alembic_cfg = Config(Path() / "alembic.ini")
alembic_cfg.set_main_option("sqlalchemy.url", source.connection_url)
alembic_cfg.attributes["connection"] = source.connection


@click.command()
def upgrade():
    command.upgrade(alembic_cfg, "head")

    command.revision(
        alembic_cfg, message=f"AUTO@{dt.datetime.now().isoformat()}", autogenerate=True
    )
    command.upgrade(alembic_cfg, "head")


# The following should be found at the end of the file
commands = click.Group(name="database")
[commands.add_command(x) for x in locals().values() if isinstance(x, click.Command)]
