import tornado
import inspect
import typing
import socket

from ..core.console import output

from . import handlers as hd
from . import ui


class AppWeb(tornado.web.Application):
    """Master application"""

    def get_list_handlers(self) -> list:
        handlers = [(r"/", hd.PH_Index)]

        "Add all the pages by their names here."
        for handler_name, handler_cls in inspect.getmembers(hd, inspect.isclass):
            if (
                issubclass(handler_cls, hd.HandlerPage)
                and handler_cls is not hd.HandlerPage
            ):
                handlers.append((handler_name, handler_cls))
        [
            output(
                source=self, message=f"{handler_name :>30} -> {handler_cls.__name__}"
            )
            for handler_name, handler_cls in handlers
        ]
        return handlers

    def get_list_ui(self) -> typing.List[ui.HandlerUI]:
        uis = []

        "Add all the pages by their names here."
        for _, item in inspect.getmembers(ui, inspect.isclass):
            if issubclass(item, ui.HandlerUI) and item is not ui.HandlerUI:
                # if issubclass(x, API) and x is not API:
                #     "API Handlers"
                #     handlers.append(
                #         ("/api{}".format(x.localUrl()), x)
                #         )
                # else:
                #     "Page handlers"
                uis.append(item)

        [output(source=self, message=f"{item !r} -> {item.__name__}") for item in uis]

        return uis

    def __init__(self):
        "Set tornado settings"
        self.get_list_ui()

        # NOTE: Following not fully implemented yet
        ssl_options = {"certfile": "cert.cer", "keyfile": "key.key"}

        tornado.web.Application.__init__(
            self,
            self.get_list_handlers(),
            **{
                # Tornado settings
                "template_path": "src/templates",
                "static_path": "static",
                "uis": ui,
                "debug": True,
                "login_url": "/login",
                "default_handler_class": hd.PH_NotFound,
            },
            cookie_secret="Super secret cookie 4",
        )

    def user_login(self, email, password):
        try:
            user = (
                ModelHandler.Session.query(CM.User)
                .filter(func.lower(CM.User.email) == email.lower())
                .one()
            )
        except sqlalchemy.orm.exc.NoResultFound:
            raise  # Exception_Login_UserNotFound(email)
        else:
            if user.password != password:
                raise  # Exception_InvalidPassword(email, password)
            else:
                "Create an api key to return"
                key = str(uuid.uuid1())
                # ModelHandler.Session.query(CM.UserAuthorization).filter_by(username=user.username).all.delete()
                # ModelHandler.Session.add(
                ua = CM.UserAuthorization(userId=user.id, key=key)
                # input(dir(ua))
                # ua.userId = user.id
                ModelHandler.Session.add(ua)
                return ua.key

    def add_websocket(self, websocket):
        assert isinstance(websocket, hd.Websocket)
        self.websockets.add(websocket)

    def serve(self, port: int = 8000):
        "Start the server"
        if tornado.ioloop.IOLoop.current():
            tornado.ioloop.IOLoop.current()

        ip = (
            (
                [
                    ip
                    for ip in socket.gethostbyname_ex(socket.gethostname())[2]
                    if not ip.startswith("127.")
                ]
                or [
                    [
                        (s.connect(("8.8.8.8", 53)), s.getsockname()[0], s.close())
                        for s in [socket.socket(socket.AF_INET, socket.SOCK_DGRAM)]
                    ][0][1]
                ]
            )
            + ["no IP found"]
        )[0]

        output(source=self, message=f"Starting at http://{ip}:{port}/")

        # Start the server
        server = tornado.httpserver.HTTPServer(self)
        # server.bind(int(port))
        # , ssl_options={
        #     "certfile": "cert.cer",
        #     "keyfile":  "key.key",
        # })
        server.listen(int(port))
        # self.startWebpack()

        # self.process_main = multiprocessing.Process(
        #     target=tornado.ioloop.IOLoop.current().start, args=()
        # )
        # self.process_main.start()
        try:
            tornado.ioloop.IOLoop.current().start()
        except KeyboardInterrupt:
            # self.process_webpack.kill()
            # print(self.process_webpack.isDaemon())
            # exit()
            quit()
            return

    # @gen.coroutine
    def chirp(self, user):
        """Look at all websockets and have them relay chirp if the client needs to be updated"""
        # If the user is in a campaign, update all users in that campaign, otherwise, chirp

        if not self.settings.get("debug", False):
            for x in self.websockets:
                if x.user == user:
                    return x.chirp()

        else:
            # DEBUG ONLY: chirp everywhere
            for x in self.websockets:
                x.chirp()
