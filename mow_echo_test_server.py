#!/usr/bin/env python

"""
Copyright (C) 2025, Sangoma Technologies Corporation
George T Joseph <gjoseph@sangoma.com>

This program is free software, distributed under the terms of
the Apache License Version 2.0.
"""

import asyncio
import io
import logging
import signal
import sys
from websockets.asyncio.server import serve
from websockets.asyncio.server import basic_auth

logger = logging.getLogger(__name__)
logging.basicConfig(
    format="%(asctime)s %(message)s",
    datefmt="[%Y-%m-%d %H:%M:%S]",
    level=logging.INFO,
)

test_failed = 0

async def send_file(ws_media, filename, chan_name, sent_buffer):
    buff_size = 1000
    f = io.open(filename, "rb", buffering=0)
    logger.info(f"Playing '{filename}' for {chan_name}")
    await ws_media.send("START_MEDIA_BUFFERING")
    while True:
        buff = f.read(buff_size)
        if buff is None or len(buff) <= 0:
            break
        # Send on the websocket
        await ws_media.send(buff)
        # Save in buffer so we can compare to what was received.
        sent_buffer.write(buff)
    f.close()
    await ws_media.send("STOP_MEDIA_BUFFERING")
    logger.info(f"Stopping '{filename}' for {chan_name}")

async def check_data(ws_media, sent_buffer, recvd_buffer,
                     optimal_frame_size, timeout):
    global test_failed
    await ws_media.send("HANGUP")
    # We need to wait a bit to receive all echoed frames.
    await asyncio.sleep(timeout)
    sent_bytes = sent_buffer.getvalue()
    sent_length = len(sent_bytes)
    received_bytes = recvd_buffer.getvalue()
    received_length = len(received_bytes)
    """
    If the file we sent wasn't an even multiple of
    optimal_frame_size, the channel driver will have padded it
    with silence before sending it to the core.  This means
    that the amount of data we get back will be greater
    than what was sent by the amount needed to fill the
    short frame.
    """
    test_failed = 0
    if (sent_length % optimal_frame_size) != 0:
        expected_length = sent_length + (optimal_frame_size - (sent_length % optimal_frame_size))
    else:
        expected_length = sent_length
    logger.info(f"Bytes sent: {sent_length} Bytes expected: {expected_length} Bytes received: {received_length}")
    if received_length < expected_length:
        logger.error("Bytes received < Bytes expected (failure)")
        test_failed = 1
    elif received_length > expected_length:
        logger.info("Bytes received > Bytes expected (ok)")
    else:
        logger.info("Bytes received == Bytes expected (ok)")


    """
    Since the received data may have been padded with silence,
    we only want to compare the first "sent_length" bytes in
    the received buffer. 
    """
    if received_bytes[0:sent_length] != sent_bytes:
        logger.error(f"Received buffer[0:{sent_length}] != sent buffer[0:{sent_length}]")
        test_failed = 1
    else:
        logger.info(f"Received buffer[0:{sent_length}] == sent buffer[0:{sent_length}]")

    sent_buffer.close()
    recvd_buffer.close()
    await ws_media.close()
    signal.raise_signal(signal.SIGTERM)

async def process_media(ws_media):
    logger.info(f"Media connected")
    chan_name = ""
    try:
        sent_buffer = io.BytesIO()
        recvd_buffer = io.BytesIO()
        chan_name = ""
        optimal_frame_size = 0;
        async for message in ws_media:
            if isinstance(message, str):
                logger.info(f"Received {message}")
                if "MEDIA_START" in message:
                    ma = message.split(" ")
                    for p in ma[1:]:
                        v = p.split(":")
                        if v[0] == "channel":
                            chan_name = v[1]
                        elif v[0] == "optimal_frame_size":
                            optimal_frame_size = int(v[1])
                    asyncio.create_task(send_file(ws_media,
                        "test.ulaw", chan_name, sent_buffer))
                if "MEDIA_BUFFERING_COMPLETED" in message:
                    asyncio.create_task(check_data(ws_media,
                        sent_buffer, recvd_buffer, optimal_frame_size, 2))
                continue
            recvd_buffer.write(message)

    except Exception as e:
        logger.info(f"Media error {e} for {chan_name}")
    logger.info(f"Media disconnected for {chan_name}")

async def main():
    try:
        async with serve(process_media, "localhost", 8787, subprotocols=["media"],
                         process_request=basic_auth(
        realm="asterisk",
        credentials=("medianame", "mediapassword"))
    ) as server:
            loop = asyncio.get_running_loop()
            loop.add_signal_handler(signal.SIGTERM, server.close)
            await server.wait_closed()
    except:
        pass

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
    logger.info(f"Test result: {test_failed} {'passed' if (test_failed == 0) else 'failed'}")
    sys.exit(test_failed)
