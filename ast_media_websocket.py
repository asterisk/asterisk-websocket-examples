"""
Copyright (C) 2025, Sangoma Technologies Corporation
George T Joseph <gjoseph@sangoma.com>

This program is free software, distributed under the terms of
the Apache License Version 2.0.
"""

import asyncio
import io
import logging
from logging import INFO, WARNING, ERROR, DEBUG, NOTSET
import traceback
from websockets.asyncio.server import serve
from websockets.asyncio.server import basic_auth
from websockets.asyncio.client import connect

class AstMediaWebSocket:
    def __init__(self, tag=None, log_level=None):
        """
        :param tag: Optional tag for logging.
        :param log_level: Optional log level.  One of logging.LOG_LEVEL.
        """
        self.logger = logging.getLogger(__name__)
        self.tag = tag
        self.optimal_frame_size = 0
        self.sending_file = False
        if log_level is not None:
            self.logger.setLevel(log_level)

    def log(self, level, message):
        """
        Logs a message with the tag.
        :param level: The logging level (e.g., info, warning, error).
        :param message: The message to log.
        """
        tag = "" if self.tag is None else f"{self.tag}: "
        self.logger.log(level, f"{tag}{message}")

    async def send_file(self, ws_media, filename, lock, sent_data=None):
        """
        Sends a file over the media websocket.
        :param ws_media: The websocket connection to send the file over.
        :param filename: The path to the file to send.
        :param lock: An asyncio lock to use to throttle outgoing data.
        :param sent_data: Optional buffer to store sent data for verification.
        """
        buff_size = 1000
        f = io.open(filename, "rb", buffering=0)
        self.log(INFO, f"Playing '{filename}'")
        await ws_media.send("START_MEDIA_BUFFERING")
        while True:
            async with lock:
                buff = f.read(buff_size)
                if buff is None or len(buff) <= 0:
                    break
                # Send on the websocket
                await ws_media.send(buff)
                if sent_data is not None:
                    sent_data.write(buff)
        f.close()
        await ws_media.send(f"STOP_MEDIA_BUFFERING {filename}")
        self.log(INFO, f"Stopping '{filename}'")

    async def echo_timer(self, ws_media, filename, timeout, lock):
        await asyncio.sleep(timeout)
        self.sending_file = True
        asyncio.create_task(self.send_file(ws_media, filename, lock))
        
    async def process_media(self, ws_media):
        """
        Processes media messages received on the websocket.
        :param ws_media: The websocket connection to process media on.
        """
        self.log(INFO, f"Media websocket connection from {ws_media.remote_address}")
        try:
            lock = asyncio.Lock()
            async for message in ws_media:
                if isinstance(message, str):
                    self.log(INFO, f"Received media notification {message}")
                    if "MEDIA_START" in message:
                        ma = message.split(" ")
                        for p in ma[1:]:
                            v = p.split(":")
                            if v[0] == "channel":
                                self.tag = v[1]
                            elif v[0] == "optimal_frame_size":
                                self.optimal_frame_size = int(v[1])
                        self.sending_file = True
                        asyncio.create_task(self.send_file(ws_media,
                            "echo-announce.ulaw", lock))
                    if "MEDIA_XOFF" in message:
                        await lock.acquire()
                    if "MEDIA_XON" in message:
                        lock.release()
                    if "MEDIA_BUFFERING_COMPLETED" in message:
                        self.sending_file = False
                        ca = message.split(" ")
                        if "test" in ca[1]:
                            await ws_media.send("HANGUP")
                            break
                        else:
                            asyncio.create_task(self.echo_timer(ws_media, 
                                "test.ulaw", 5, lock))
                    continue
                if not self.sending_file:
                    await ws_media.send(message)
        except Exception as e:
            self.log(ERROR, f"Media error {e}")
            traceback.print_exc()
            raise e
        finally:
            self.log(INFO, f"Media disconnected")

class AstMediaWebSocketServer(AstMediaWebSocket):
    def __init__(self, host, port, credentials, protocol, tag=None, log_level=None):
        """
        Initializes the media websocket server.
        :param host: The host address to bind the server to.
        :param port: The port to bind the server to.
        :param credentials: Optional credential tuple for authentication ("username", "password")
        :param protocol: The protocol to use for the websocket. Default "media".
        :param tag: Optional tag for logging.
        """
        super().__init__(tag, log_level)
        self.host = host
        self.port = port
        self.protocol = protocol
        self.credentials = credentials
        self.server = None

    async def listen(self):
        """
        Starts the media websocket server and listens for incoming connections.
        """
        self.log(INFO, f"Starting media server")
        auth = None
        if self.credentials is not None:
            auth =  basic_auth(realm="asterisk", credentials=self.credentials)

        async with serve(self.process_media, self.host, self.port,
                         subprotocols=[self.protocol],
                         process_request=auth) as server:
            self.server = server
            await server.wait_closed()

    async def stop(self):
        if self.server is not None:
            self.log(INFO, "Stopping Media server")
            self.server.close()
            await self.server.wait_closed()
            self.server = None

class AstMediaWebSocketClient(AstMediaWebSocket):
    def __init__(self, host, port, connection_id, tag=None, log_level=None):
        """
        Initializes the media websocket client.
        :param uri: The URI to connect to the media websocket.
        :param tag: Optional tag for logging.
        """
        super().__init__(tag, log_level)
        self.host = host
        self.port = port
        self.connection_id = connection_id

    async def connect(self):
        """
        Connects to the media websocket and starts processing media.
        """
        uri = f"ws://{self.host}:{self.port}/media/{self.connection_id}"
        self.log(INFO, f"Connecting to Media at {uri}")
        async with connect(uri, subprotocols=["media"]) as ws_media:
            await self.process_media(ws_media)

