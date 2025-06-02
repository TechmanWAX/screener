import asyncio
import json
import os
import time

import logging
import simplefix

import utils.data as td


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('FixClient')


class AsyncFixClient:
    """Asynchronous client for the FIX protocol using simplefix for message parsing."""

    def __init__(self, heartbeat_interval: int = 30):
        """Initialize the FIX client with heartbeat interval."""
        self.md = {}
        self.ctrader_requests = {
            "1678": "EURUSD",
            "1718": "XAUUSD",
            "1719": "XAGUSD",
            "1766": "XPDUSD",
            "1767": "XPTUSD",
            "1786": "SP500USD",
            "1787": "NAS100USD",
        }
        for asset in self.ctrader_requests:
            guid = td.trading_data.generate_guid(asset)
            self.ctrader_requests[asset] = guid
            for symbol, asset in self.ctrader_requests.items():
                self.md[asset] = {
                    "guid": guid,
                    "bids": [],
                    "asks": [],
                    "timestamp": '',
                }

        self.config_file = 'config.json'
        if not os.path.exists(self.config_file):
            raise FileNotFoundError('Config file not found')
        with open(self.config_file, 'r') as f:
            self.config = json.load(f)

        self.sender_comp_id = self.config['SenderCompID']
        self.username = self.config['Username']
        self.password = self.config['Password']
        self.target_comp_id = 'cServer'
        self.sender_sub_id = 'QUOTE'
        self.target_sub_id = 'QUOTE'
        self.host = 'live-uk-eqx-02.p.ctrader.com'
        self.port = 5201
        self.heartbeat_interval = heartbeat_interval

        self.reader = None
        self.writer = None
        self.seq_num = 1
        self.parser = simplefix.FixParser()
        self.stay_connected = False
        self.heartbeat_task = None

    async def connect(self) -> None:
        """Establish connection to the FIX server and start heartbeat."""
        logger.info(f"Connecting to {self.host}:{self.port}")
        try:
            # Wrap connection attempt with a timeout
            self.reader, self.writer = await asyncio.wait_for(
                asyncio.open_connection(self.host, self.port),
                timeout=10.0  # 10-second timeout
            )
        except asyncio.TimeoutError:
            logger.error(f"Connection to {self.host}:{self.port} timed out after 10 seconds")
            raise  # Re-raise to halt execution and signal failure
        except ConnectionRefusedError:
            logger.error(f"Connection refused by {self.host}:{self.port}")
            raise
        except OSError as e:
            logger.error(f"Network error connecting to {self.host}:{self.port}: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error connecting to {self.host}:{self.port}: {e}")
            raise

        await self.send_logon()
        self.stay_connected = True
        self.heartbeat_task = asyncio.create_task(self.heartbeat_loop())
        logger.info("Connection established")

    async def heartbeat_loop(self) -> None:
        """Periodically send heartbeat messages."""
        while self.stay_connected:
            await asyncio.sleep(self.heartbeat_interval)
            if self.stay_connected:
                await self.send_heartbeat()

    async def send_heartbeat(self) -> None:
        """Send a heartbeat message."""
        print(f"Debug forex market data: {td.trading_data.order_book}")
        msg = self._create_base_message("0")
        await self.send_message(msg)

    async def send_logon(self) -> None:
        """Send a logon message."""
        msg = self._create_base_message("A")
        msg.append_pair(98, 0)  # EncryptMethod = None
        msg.append_pair(108, self.heartbeat_interval)
        msg.append_pair(141, 'Y')  # ResetSeqNumFlag
        msg.append_pair(553, self.username)
        msg.append_pair(554, self.password)
        await self.send_message(msg)
        logger.info(f"Logon message sent: {msg}")

    async def send_logout(self) -> None:
        """Send a logout message."""
        msg = self._create_base_message("5")
        await self.send_message(msg)
        logger.info(f"Logout message sent: \n{msg}")

    def _create_base_message(self, msg_type: str) -> simplefix.FixMessage:
        """Create a base FIX message with common fields."""
        msg = simplefix.FixMessage()
        msg.append_pair(8, "FIX.4.4")
        msg.append_pair(35, msg_type)
        msg.append_pair(49, self.sender_comp_id)
        msg.append_pair(56, self.target_comp_id)
        if self.sender_sub_id:
            msg.append_pair(50, self.sender_sub_id)
        if self.target_sub_id:
            msg.append_pair(57, self.target_sub_id)
        msg.append_pair(34, self.seq_num)
        self.seq_num += 1
        msg.append_utc_timestamp(52)
        return msg

    async def send_message(self, msg: simplefix.FixMessage) -> None:
        """Send a FIX message."""
        if not self.writer:
            raise ConnectionError("Not connected to server")
        encoded_msg = msg.encode()
        self.writer.write(encoded_msg)
        await self.writer.drain()

    async def listen(self) -> None:
        """Listen for incoming FIX messages."""
        if not self.reader:
            raise ConnectionError("Not connected to server")
        self.listening = False
        buffer = bytearray()

        while self.stay_connected:
            try:
                # Read data
                data = await self.reader.read(4096)
                if not data:
                    logger.warning("Connection closed by server")
                    self.stay_connected = False
                    break

                # Add to buffer
                buffer.extend(data)

                # Try to parse complete messages from the buffer
                messages_found = True
                while messages_found and buffer:
                    # Create a new parser for each iteration to ensure clean state
                    temp_parser = simplefix.FixParser()
                    temp_parser.append_buffer(bytes(buffer))
                    msg = temp_parser.get_message()

                    if msg is None:
                        # No complete message found in buffer
                        messages_found = False
                    else:
                        # Process the message
                        await self.process_message(msg)

                        # Find the end of this message to remove it from buffer
                        # FIX messages end with the SOH character (ASCII 1)
                        msg_bytes = msg.encode()
                        msg_len = len(msg_bytes)
                        if msg_len <= len(buffer):
                            buffer = buffer[msg_len:]
                        else:
                            # This shouldn't happen, but just in case
                            logger.warning("Message length issue, clearing buffer")
                            buffer.clear()
                if not self.listening:
                    self.listening = True  # Set after first successful read
                # print(self.md)
            except asyncio.CancelledError:
                logger.info("Listen task cancelled")
                self.stay_connected = False
                break
            except Exception as e:
                logger.error(f"Error in listen loop: {e}")
                await asyncio.sleep(1)  # Prevent busy looping on error

    async def process_message(self, msg: simplefix.FixMessage) -> None:
        """Process received FIX messages."""
        msg_type = msg.get(35).decode('ascii')
        if msg_type == '0':  # Heartbeat
            pass
        elif msg_type == '1':  # Test request
            test_req_id = msg.get(112)
            if test_req_id:
                await self.send_heartbeat_response(test_req_id)
        elif msg_type == '5':  # Logout
            self.stay_connected = False
        elif msg_type == 'W':  # Market Data Snapshot
            await self.handle_market_data_snapshot(msg)
        elif msg_type == 'X':  # Market Data Incremental Refresh
            await self.handle_market_data_incremental(msg)
        # print(self.md)

    async def send_heartbeat_response(self, test_req_id: bytes) -> None:
        """Send heartbeat response to a test request."""
        msg = self._create_base_message("0")
        msg.append_pair(112, test_req_id)
        await self.send_message(msg)

    async def handle_market_data_snapshot(self, msg: simplefix.FixMessage) -> None:
        """Process Market Data Snapshot message."""
        try:
            symbol = msg.get(55)
            if symbol:
                symbol = symbol.decode('ascii')

            # Extract entry count
            no_entries = msg.get(268)
            if not no_entries:
                return
            no_entries = int(no_entries.decode('ascii'))

            for i in range(no_entries):
                entry_type = msg.get(269, i)
                if entry_type:
                    entry_type = entry_type.decode('ascii')
                price = msg.get(270, i)
                if price:
                    price = float(price.decode('ascii'))
                size = msg.get(271, i)
                if size:
                    size = float(size.decode('ascii'))

                for key, value in self.ctrader_requests.items():
                    if symbol == key:
                        if entry_type == "0":  # Bid
                            for entry in self.md[value]['bids']:
                                if entry['volume'] == size:
                                    entry['price'] = price
                                    break
                            else:
                                self.md[value]['bids'].append({"price": price, "volume": size})
                        elif entry_type == "1":  # Offer
                            for entry in self.md[value]['asks']:
                                if entry['volume'] == size:
                                    entry['price'] = price
                                    break
                            self.md[value]['asks'].append({"price": price, "volume": size})
                        td.trading_data.order_book[value] = td.OrderBook(
                            instrument_uid=value,
                            bids=self.md[value].get('bids', []),
                            asks=self.md[value].get('asks', []),
                            timestamp=int(time.time()),
                        )
                        break
        except Exception as e:
            logger.error(f"Error processing snapshot: {e}")

    async def handle_market_data_incremental(self, msg: simplefix.FixMessage) -> None:
        """Process Market Data Incremental Refresh message."""
        try:
            no_entries = msg.get(268)
            if not no_entries:
                return
            no_entries = int(no_entries.decode('ascii'))

            for i in range(no_entries):
                # Update action (0=New, 1=Change, 2=Delete)
                update_action = msg.get(279, i)
                if update_action:
                    update_action = update_action.decode('ascii')

                # Entry type (0=Bid, 1=Offer, etc.)
                entry_type = msg.get(269, i)
                type_str = "Unknown"
                if entry_type:
                    entry_type = entry_type.decode('ascii')
                symbol = msg.get(55, i)
                if symbol:
                    symbol = symbol.decode('ascii')
                price = msg.get(270, i)
                if price:
                    price = float(price.decode('ascii'))
                size = msg.get(271, i)
                if size:
                    size = float(size.decode('ascii'))
                # if update_action == "0":  # New
                for key, value in self.ctrader_requests.items():
                    if symbol == key:
                        if entry_type == "0":  # Bid
                            for entry in self.md[value]['bids']:
                                if entry['volume'] == size:
                                    entry['price'] = price
                                    break
                            else:
                                self.md[value]['bids'].append({"price": price, "volume": size})
                        elif entry_type == "1":  # Offer
                            for entry in self.md[value]['asks']:
                                if entry['volume'] == size:
                                    entry['price'] = price
                                    break
                            else:
                                self.md[value]['asks'].append({"price": price, "volume": size})
                        td.trading_data.order_book[value] = td.OrderBook(
                            instrument_uid=value,
                            bids=self.md[value].get('bids', []),
                            asks=self.md[value].get('asks', []),
                            timestamp=int(time.time()),
                        )
                        break

        except Exception as e:
            logger.error(f"Error processing incremental data: {e}")

    async def disconnect(self) -> None:
        """Disconnect from the FIX server."""
        self.stay_connected = False
        if self.heartbeat_task:
            self.heartbeat_task.cancel()
            try:
                await self.heartbeat_task
            except asyncio.CancelledError:
                pass
        if self.writer:
            await self.send_logout()
            self.writer.close()
            await self.writer.wait_closed()
        logger.info("Disconnected from FIX server")

    async def request_market_data(self) -> None:
        """Send Market Data Requests for all symbols."""
        now = time.time()
        for request in self.ctrader_requests:
            now += 1
            md_req_id = str(now)
            msg = self._create_base_message("V")
            msg.append_pair(262, md_req_id)
            msg.append_pair(263, "1")  # Snapshot + updates
            msg.append_pair(264, 0)  # Full book
            msg.append_pair(265, 1)  # Incremental
            msg.append_pair(267, 2)  # Number of MDEntryTypes
            msg.append_pair(269, "0")  # Bid
            msg.append_pair(269, "1")  # Offer
            msg.append_pair(146, "1")  # NoRelatedSym
            msg.append_pair(55, request)
            await self.send_message(msg)
            logger.info(f"Market data request sent for symbol: {request}")
            await asyncio.sleep(0.1)

    async def start(self) -> None:
        """Start the FIX client and data acquisition."""
        try:
            await self.connect()
        except Exception as e:
            logger.error(f"Error connecting to FIX server: {e}")
            return
        await asyncio.sleep(0.1)
        listen_task = asyncio.create_task(self.listen())
        while not getattr(self, 'listening', False):
            await asyncio.sleep(0.1)
        await self.request_market_data()


async def main():
    """Main function to create client and start data acquisition."""
    client = AsyncFixClient()
    await client.start()
    while client.stay_connected:
        await asyncio.sleep(1)


if __name__ == "__main__":
    asyncio.run(main())
