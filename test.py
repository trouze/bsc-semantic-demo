import asyncio
import os

from dbtsl.asyncio import AsyncSemanticLayerClient


async def main():
    client = AsyncSemanticLayerClient(
        environment_id=os.getenv("DBT_ENV_ID"),
        auth_token=os.getenv("DBT_TOKEN"),
        host=os.getenv("DBT_HOST"),
    )
    async with client.session():
        metrics = await client.metrics()
        for m in metrics:
            print(m.name)


asyncio.run(main())
