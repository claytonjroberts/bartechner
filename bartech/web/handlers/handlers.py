import inspect

import sys


import tornado
import tornado.autoreload
import tornado.websocket


from ...core.console import output


_debug = True


from .base import HandlerPage, HandlerAPI, HandlerWebsocket


class PH_Logout(HandlerPage):
    def get(self):
        # ModelHandler.Session.query(CM.UserAuthorization).filter_by(username=self.get_current_user_obj().username).delete()
        self.clear_cookie("userkey")
        self.redirect("/")


class PH_Admin(HandlerPage):
    @tornado.web.authenticated
    def get(self):
        return super().get()

    @property
    def adminPHClasses(self):
        final = []
        for y in inspect.getmembers(sys.modules[__name__], inspect.isclass):
            x = y[1]
            if issubclass(x, PH_Admin) and x is not PH_Admin:
                final.append(x)
        return final

    @classmethod
    def getIsTool(cls):
        return issubclass(cls, HandlerAPI)


if __name__ == "__main__":
    pass
