from pathlib import Path

import flask

from flask import render_template


class App(flask.Flask):
    def __init__(self, import_name):
        super().__init__("BarTech", template_folder=[Path() / "src" / "templates"])

        # Set up some logging and template paths.
        self.register_error_handler(404, self.my_404)
        self.before_request(self.my_preprocessing)

        self.route("/")(lambda: render_template("index.html", title="Home", user=user))

    def my_404(self, error):
        return flask.render_template("404.html"), 404

    def my_preprocessing(self):
        # Do stuff to flask.request
        pass
