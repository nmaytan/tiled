import asyncio
from pathlib import Path

from tiled.access_control.access_tags import AccessTagsCompiler
from tiled.access_control.scopes import ALL_SCOPES
from tiled.server.connection_pool import close_database_connection_pool
from tiled.server.settings import DatabaseSettings


def group_parser(groupname):
    return {
        "group_A": ["alice", "bob"],
        "admins": ["cara"],
    }[groupname]


async def main():
    file_directory = Path(__file__).resolve().parent

    # The compiler writes the tag definitions into the catalog database,
    # where the tiled server (its AccessTagsParser) reads them. This is the
    # same database configured under 'trees' in toy_authentication.yml.
    # Run this script before example_configs/catalog/create_catalog.py:
    # create_catalog.py applies access tags to the data it writes, and tags
    # must be defined (compiled) before they can be applied. The compiler
    # creates the access tag tables itself if the catalog database does not
    # exist yet.
    catalog_database = file_directory.parent / "catalog" / "catalog.db"
    database_settings = DatabaseSettings(uri=f"sqlite+aiosqlite:///{catalog_database}")

    access_tags_compiler = AccessTagsCompiler(
        ALL_SCOPES,
        Path(file_directory, "tag_definitions.yml"),
        database_settings,
        group_parser,
    )

    access_tags_compiler.load_tag_config()
    await access_tags_compiler.compile()
    await close_database_connection_pool(database_settings)


if __name__ == "__main__":
    asyncio.run(main())
