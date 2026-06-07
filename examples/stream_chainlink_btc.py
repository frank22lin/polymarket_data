"""Stream Chainlink BTC/USD Data Streams reports.

Required environment:
    STREAMS_API_KEY
    STREAMS_API_SECRET
    CHAINLINK_BTC_USD_FEED_ID

Optional:
    STREAMS_WS_HOST

Install websocket support first:
    pip install -e ".[chainlink]"
"""

from polymarket_data.chainlink_stream import main


if __name__ == "__main__":
    main()
