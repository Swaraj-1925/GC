import asyncio
import sys

# Add parent to path for imports
sys.path.insert(0, '.')

from src.shared.config import get_settings, RedisKeys
from src.shared.db.redis_client import RedisClient


async def clear_all_data():
    """Clear all GemScap data from Redis."""
    settings = get_settings()
    redis = RedisClient("clear_script")

    try:
        await redis.connect()
        # print(f"Connected to Redis: {settings.REDIS_HOST}:{settings.REDIS_PORT}")

        # Delete tick streams
        for symbol in settings.SYMBOLS:
            stream_key = RedisKeys.tick_stream(symbol.upper())
            try:
                deleted = await redis._client.delete(stream_key)
                if deleted:
                    print(f"✓ Deleted stream: {stream_key}")
            except Exception as e:
                print(f"✗ Error deleting {stream_key}: {e}")

        # Delete analytics state hashes
        for symbol in settings.SYMBOLS:
            state_key = RedisKeys.analytics_state(symbol.upper())
            try:
                deleted = await redis._client.delete(state_key)
                if deleted:
                    print(f"✓ Deleted state: {state_key}")
            except Exception as e:
                print(f"✗ Error deleting {state_key}: {e}")

        # Delete price TimeSeries
        for symbol in settings.SYMBOLS:
            ts_key = RedisKeys.price_timeseries(symbol.upper())
            try:
                deleted = await redis._client.delete(ts_key)
                if deleted:
                    print(f"✓ Deleted timeseries: {ts_key}")
            except Exception as e:
                print(f"✗ Error deleting {ts_key}: {e}")

        # Delete alerts
        try:
            keys = await redis._client.keys("alert:*")
            if keys:
                deleted = await redis._client.delete(*keys)
                print(f"✓ Deleted {deleted} alert keys")

            await redis._client.delete("alerts:active")
            print(f"✓ Cleared alerts:active set")
        except Exception as e:
            print(f"✗ Error deleting alerts: {e}")

        print("\n✅ Redis data cleared successfully!")
        print("You can now run: python main.py")

    except Exception as e:
        print(f"Error: {e}")
    finally:
        await redis.disconnect()


if __name__ == "__main__":
    print("=== GemScap Redis Data Cleaner ===\n")
    asyncio.run(clear_all_data())
