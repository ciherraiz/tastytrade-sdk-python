import logging
import threading
import time
from itertools import product
from math import floor
from typing import Callable, Optional

import ujson
from websockets.exceptions import ConnectionClosedOK
from websockets.sync.client import connect, ClientConnection

from tastytrade_sdk.exceptions import TastytradeSdkException, InvalidArgument
from tastytrade_sdk.market_data.streamer_symbol_translation import StreamerSymbolTranslations


class LoopThread(threading.Thread):
    def __init__(self, activity: Callable, timeout_seconds: int = 0):
        threading.Thread.__init__(self)
        self.__running = True
        self.__activity = activity
        self.__timeout_seconds = timeout_seconds
        super().start()

    def run(self):
        while self.__running:
            self.__activity()
            self.__pause()

    def __pause(self):
        if not self.__timeout_seconds:
            return
        start = time.time()
        while self.__running and time.time() - start <= self.__timeout_seconds:
            continue

    def stop(self):
        self.__running = False


class Subscription:
    __websocket: Optional[ClientConnection] = None
    __keepalive_thread: Optional[LoopThread]
    __receive_thread: Optional[LoopThread]
    __is_authorized: bool = False

    def __init__(self, url: str, token: str, streamer_symbol_translations: StreamerSymbolTranslations,
                 on_candle: Callable[[dict], None] = None,
                 on_greeks: Callable[[dict], None] = None,
                 on_quote: Callable[[dict], None] = None
                 ):
        """@private"""

        if not (on_quote or on_candle or on_greeks):
            raise InvalidArgument('At least one feed event handler must be provided')

        self.__url = url
        self.__token = token
        self.__streamer_symbol_translations = streamer_symbol_translations
        self.__on_quote = on_quote
        self.__on_candle = on_candle
        self.__on_greeks = on_greeks

    def open(self) -> 'Subscription':
        """Start listening for feed events"""
        self.__websocket = connect(self.__url)
        self.__receive_thread = LoopThread(self.__receive)

        subscription_types = []
        if self.__on_quote:
            subscription_types.append('Quote')
        if self.__on_candle:
            subscription_types.append('Candle')
        if self.__on_greeks:
            subscription_types.append('Greeks')

        subscriptions = [{'symbol': s, 'type': t} for s, t in
                         product(self.__streamer_symbol_translations.streamer_symbols, subscription_types)]

        self.__send('SETUP', version='0.1-js/1.0.0')
        self.__send('AUTH', token=self.__token)
        while not self.__is_authorized:
            continue
        self.__send('CHANNEL_REQUEST', channel=1, service='FEED', parameters={'contract': 'AUTO'})
        self.__send('FEED_SUBSCRIPTION', channel=1, add=subscriptions)
        return self

    def close(self) -> None:
        """Close the stream connection"""
        if self.__keepalive_thread:
            self.__keepalive_thread.stop()
        if self.__receive_thread:
            self.__receive_thread.stop()
        if self.__websocket:
            self.__websocket.close()

    def __receive(self) -> None:
        if not self.__websocket:
            return
        try:
            message = ujson.loads(self.__websocket.recv())
        except ConnectionClosedOK:
            return
        _type = message['type']
        if _type == 'ERROR':
            raise StreamerException(message['error'], message['message'])
        if _type == 'SETUP':
            keepalive_interval = floor(message['keepaliveTimeout'] / 2)
            self.__keepalive_thread = LoopThread(lambda: self.__send('KEEPALIVE'), keepalive_interval)
        elif _type == 'AUTH_STATE':
            self.__is_authorized = message['state'] == 'AUTHORIZED'
        elif _type == 'FEED_DATA':
            self.__handle_feed_event(message['data'])
        else:
            logging.debug('Unhandled message type: %s', _type)

    def __handle_feed_event(self, event: dict) -> None:
        event_type = event[0]
        if event_type == 'Quote' and self.__on_quote:
            data = self.__handle_compact_quote(event[1])
            self.__on_quote(data)
        elif event_type == 'Candle' and self.__on_candle:
            data = self.__handle_compact_candle(event[1])
            self.__on_candle(data)
        elif event_type == 'Greeks' and self.__on_greeks:
            data = self.__handle_compact_greeks(event[1])
            self.__on_greeks(data)
        else:
            logging.debug('Unhandled feed event type %s for symbol %s', event_type)
    
    def __handle_compact_quote(self, data: list) -> list:
        quote = {}
        for i in range(0, len(data), 13):
            event_symbol = data[i+1]
            original_symbol = self.__streamer_symbol_translations.get_original_symbol(event_symbol)
            quote['Symbol'] = original_symbol
            quote['eventSymbol'] = event_symbol
            quote['bidPrice'] = data[i+7]
            quote['askPrice'] = data[i+11]
            quote['timeStamp'] = time.time()
        return quote

    def __handle_compact_candle(self, data: list) -> list:
        candle = {}
        for i in range(0, len(data), 18):
            event_symbol = data[i+1]
            original_symbol = self.__streamer_symbol_translations.get_original_symbol(event_symbol)
            candle['Symbol'] = original_symbol
            candle['eventSymbol'] = event_symbol
            candle['Open'] = data[i+8]
            candle['High'] = data[i+9]
            candle['Low'] = data[i+10]
            candle['Close'] = data[i+11]
            candle['Volumen'] = data[i+12]
            candle['timeStamp'] = time.time()
        return candle
    
    def __handle_compact_greeks(self, data: list) -> list:
        greeks = {}
        for i in range(0, len(data), 14):
            event_symbol = data[i+1]
            original_symbol = self.__streamer_symbol_translations.get_original_symbol(event_symbol)
            greeks['Symbol'] = original_symbol
            greeks['eventSymbol'] = event_symbol
            greeks['Price'] = data[i+8]
            greeks['Volatility'] = data[i+9]
            greeks['Delta'] = data[i+10]
            greeks['Gamma'] = data[i+11]
            greeks['Theta'] = data[i+12]
            greeks['Rho'] = data[i+13]
            greeks['Vega'] = data[i+13]
            greeks['timeStamp'] = time.time()
        return greeks

    def __send(self, _type: str, channel: Optional[int] = 0, **kwargs) -> None:
        self.__websocket.send(ujson.dumps({
            **{'type': _type, 'channel': channel},
            **kwargs
        }))


class StreamerException(TastytradeSdkException):
    def __init__(self, error: str, message: str):
        super().__init__(f'{error}: {message}')
