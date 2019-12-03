import click

import inquirer
import re


class App:
    def main(self):
        questions = [
            inquirer.Text("name", message="What's your name"),
            inquirer.Text("surname", message="What's your surname"),
            inquirer.Text(
                "phone",
                message="What's your phone number",
                validate=lambda _, x: re.match("\+?\d[\d ]+\d", x),
            ),
            # inquirer.Editor("long_text", message="Provide long text"),
        ]
        print(inquirer.prompt(questions))


if __name__ == "__main__":
    App().main()
