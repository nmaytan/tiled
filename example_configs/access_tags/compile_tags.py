import asyncio
from pathlib import Path

from tiled.access_control.access_tags import AccessTagsCompiler
from tiled.server.connection_pool import close_database_connection_pool
from tiled.server.settings import DatabaseSettings

# The valid scopes for compilation. The compiler and the access policy must
# be fed the same scope list: the scopes in the catalog database must be a
# subset of the policy's scopes (the server warns at startup otherwise), and
# the policy ignores any tag grant that is not a pure subset of its scopes.
# This list matches the policy 'scopes' in toy_authentication.yml.
SCOPES = [
    "read:metadata",
    "read:data",
    "write:metadata",
    "write:data",
    "delete:revision",
    "delete:node",
    "create:node",
    "register",
]


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
        SCOPES,
        Path(file_directory, "tag_definitions.yml"),
        database_settings,
        group_parser,
    )

    access_tags_compiler.load_tag_config()
    await access_tags_compiler.compile()
    await close_database_connection_pool(database_settings)


if __name__ == "__main__":
    asyncio.run(main())
