import inspect
import json
import re
import sys
import time
import uuid
from collections import defaultdict
from pathlib import Path

import sqlalchemy
import tornado
import tornado.autoreload
import tornado.websocket

from sqlalchemy import and_, func, or_
from sqlalchemy.orm.exc import NoResultFound
from tornado import gen
from tornado.ioloop import IOLoop, PeriodicCallback

from ...core.console import output
from ..constants import PATH_TEMPLATES
from ..helpers import get_path_from_name

_debug = True


class HandlerWebsocket(tornado.websocket.WebSocketHandler):
    "Only one of these should be instanciated at a time"

    count = 0

    @gen.coroutine
    def open(self):
        self.application.addWebsocket(self)
        output(source=self, message="WebSocket opened [_]")
        self.chirp()

    @gen.coroutine
    def chirp(self):
        output(source=self, message="CAW")
        """Send message to client to update."""
        self.write_message("chirp")

    # @gen.coroutine
    # def on_message(self, message):
    #     # output(source = self, message = "[RECIEVED] {}".format(message))
    #
    #     cargo = Cargo.loadData(message)
    #
    #     output(source = self, message = "[RECIEVED] {}".format(cargo.__repr__()))
    #
    #     return self.send(self.application.parse(cargo, self))

    def on_close(self):
        output(source=self, message="WebSocket closed [X]")

    # @gen.coroutine
    # def send(self, cargo):
    #     try:
    #         assert(isinstance(cargo, Cargo))
    #     except AssertionError:
    #         print(type(cargo))
    #         quit()
    #     self.write_message(cargo.wrap())
    #     output(source = self, message = "[SENT] {}".format(repr(cargo)))


class HandlerPage(tornado.web.RequestHandler):
    "Base class for request handlers"

    @gen.coroutine
    def get(self, *args, **kwargs):

        self.render(
            get_path_from_name(
                name="_".join(self.__class__.__name__.split("_")[1:]),
                path=PATH_TEMPLATES,
            ).name
        )

    @classmethod
    def title(cls):
        return " ".join(cls.__name__.split("_")[1:]).lower()

    @classmethod
    def localUrl(cls):
        return "/" + "/".join(cls.__name__.lower().split("_")[1:])

    def raiseError(self, error):
        """Set the error and re-load the page."""
        self.error = error
        self.get()

    def write_error(self, status_code, **kwargs):
        print("writing an error")

        if status_code == 404:
            return PH_Unauthorized.get()

        super().write_error(status_code, **kwargs)

    def initialize(self):
        """Set variables for PH."""
        # self.ModelHandler = (
        #     ModelHandler()
        # )  # TODO: Add ip or some identifying factor to the start of the ModelHandler object
        self.error = None

    @gen.coroutine
    def chirp(self):
        return self.application.chirp(self.user)


class HandlerAPI(HandlerPage):
    @classmethod
    def localUrl(cls):
        return "/api{}".format(super().localUrl())
